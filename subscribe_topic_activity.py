import json
from basic_activity import BasicActivity
import boto3

#
# **SubscribeTopicActivity** sends an SMS / email message to the user, asking for
# confirmation.  When this action has been taken, the activity is complete.
#
class SubscribeTopicActivity(BasicActivity):
  
  # initialize the activity
  def __init__(self):
    super(SubscribeTopicActivity, self).__init__('SubscribeTopicActivity', 'v1')
 
  # Create an SNS topic and return the ARN
  def create_topic(self, sns_client):
    topic_arn = sns_client.create_topic(Name='SWF_Sample_Topic')["TopicArn"]

    if topic_arn != None:
      # For an SMS notification, setting `DisplayName` is *required*. Note that
      # only the *first 10 characters* of the DisplayName will be shown on the
      # SMS message sent to the user, so choose your DisplayName wisely!
      sns_client.set_topic_attributes(
        TopicArn = topic_arn,
        AttributeName = 'DisplayName',
        AttributeValue = 'SWFSample' )
    else:
      self.results = json.dumps({"reason": "Couldn't create SNS topic", "detail": "" })
      return None

    return topic_arn
    
  # Attempt to subscribe the user to an SNS Topic.
  def do_activity(self, task):
    activity_data = {
      "topic_arn": None,
      "email": { "endpoint": None, "subscription_arn": None },
      "sms": { "endpoint": None, "subscription_arn": None },
    }

    if task["input"] != None:
      input = json.loads(task["input"])
      activity_data["email"]["endpoint"] = input["email"]
      activity_data["sms"]["endpoint"] = input["sms"]
    else:
      self.results = json.dumps({ "reason": "Didn't receive any input!", "detail": "" })
      print(results)
      return False

    # Create an SNS client. This is used to interact with the service. 
    sns_client = boto3.client('sns')

    # Create the topic and get the ARN
    activity_data["topic_arn"] = self.create_topic(sns_client)

    if activity_data["topic_arn"] is None:
      return False

    # Subscribe the user to the topic, using either or both endpoints.
    for x in ["email", "sms"]:
      ep = activity_data[x]["endpoint"]
      # don't try to subscribe an empty endpoint
      if (ep != None and ep != ""):
        response = sns_client.subscribe(
          TopicArn= activity_data["topic_arn"],
          Protocol= x, Endpoint= ep )
        activity_data[x]["subscription_arn"] = response["SubscriptionArn"]

    # if at least one subscription arn is set, consider this a success.
    if (activity_data["email"]["subscription_arn"] != None) or (activity_data["sms"]["subscription_arn"] != None):
      self.results = json.dumps(activity_data)
    else:
      self.results = json.dumps({ "reason": "Couldn't subscribe to SNS topic", "detail": "" })
      print(results)
      return False

    return True
