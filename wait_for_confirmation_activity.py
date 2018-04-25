import json
from basic_activity import BasicActivity
import boto3

import time

#
# **WaitForConfirmationActivity** waits for the user to confirm the SNS
# subscription.  When this action has been taken, the activity is complete. It
# might also time out...
#
class WaitForConfirmationActivity(BasicActivity):

  # initialize the activity
  def __init__(self):
    super(WaitForConfirmationActivity, self).__init__('WaitForConfirmationActivity', 'v1')

  # confirm the SNS topic subscription
  def do_activity(self, task):
    if task["input"] is None:
      self.results = json.dumps({ "reason": "Didn't receive any input!", "detail": "" })
      return False

    subscription_data = json.loads(task["input"])
    print("Subscription Data is: %s" % subscription_data)

    # get the SNS topic by using the ARN retrieved from the previous activity,
    # so we can check to see if the user has confirmed the subscription.
    
    sns_client = boto3.client('sns')
    topic = sns_client.get_topic_attributes(TopicArn=subscription_data["topic_arn"])

    if topic:
        subscription_confirmed = False
    else:
        self.results = json.dumps({ "reason" : "Couldn't get SWF topic ARN", "detail" : "Topic ARN: %s" % subscription_data["topic_arn"] })
        return False
        
    # Loop through all of the subscriptions to this topic until we get some indication that a subscription was confirmed.
    while not subscription_confirmed:
        for sub in sns_client.list_subscriptions_by_topic(TopicArn=subscription_data["topic_arn"])["Subscriptions"]:
            print("Subcriptions by Topic items is: %s" % json.dumps(sub))
            if subscription_data[sub["Protocol"]]["endpoint"] == sub["Endpoint"]:
                # this is one of the endpoints we're interested in. Is it subscribed?
                if sub["SubscriptionArn"] != 'PendingConfirmation':
                    subscription_data[sub["Protocol"]]["subscription_arn"] = sub["SubscriptionArn"]
                    subscription_confirmed = True
                    
        
        swf_client = boto3.client('swf')  
        response = swf_client.record_activity_task_heartbeat(
            taskToken=task['taskToken'],
            details='Waiting for subscriptions confirmation'
        )
        
        print("Sleeping while waiting for subscription confirmation.")
        time.sleep(4)

    self.results = json.dumps(subscription_data)
    
    return True