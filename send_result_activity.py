#
# **SendResultActivity** sends the result of the activity to the screen, and, if
# the user successfully registered using SNS, to the user using the SNS contact
# information collected.
#
import json
from basic_activity import BasicActivity

import boto3

class SendResultActivity(BasicActivity):
  
  # initialize the activity
  def __init__(self):
   super(SendResultActivity, self).__init__('SendResultActivity', 'v1')

  # confirm the SNS topic subscription
  def do_activity(self, task):
    if task:
        subscription_data = json.loads(task["input"])
    else:
        self.results = json.dumps({"reason", "Didn't receive any input!", "detail", "" })
        return
    
    sns_client = boto3.client('sns')
    results = "Thanks, you've successfully confirmed registration, and your workflow is complete!"
    
    # send the message via SNS
    sns_client.publish(TopicArn=subscription_data["topic_arn"], Message=results)
    self.results=results
    return True
