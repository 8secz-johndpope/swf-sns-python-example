import boto3
from botocore.client import Config

from utils import init_domain

import get_contact_activity
import subscribe_topic_activity
import wait_for_confirmation_activity
import send_result_activity

import sys

import uuid
import json

botoConfig = Config(connect_timeout=50, read_timeout=70)
swf = boto3.client('swf', config=botoConfig)

class ActivitiesWorker:

  def __init__(self, domain, task_list):
    self.domain = domain
    self.task_list = task_list
    self.activities = {}

    # These are the activities we'll run
    activity_sequence = [
      get_contact_activity.GetContactActivity,
      subscribe_topic_activity.SubscribeTopicActivity,
      wait_for_confirmation_activity.WaitForConfirmationActivity,
      send_result_activity.SendResultActivity ]

    for activity_class in activity_sequence:
      activity_obj = activity_class()
      print( "** initialized and registered activity: %s" % activity_class.__name__ )
      # add it to the hash
      self.activities[activity_class.__name__] = activity_obj

  #
  # Polls for activities until the activity is marked complete.
  #
  def poll_for_activities(self):
    print("Listening for Worker Tasks: %s" % self.task_list )

    while True:
      task = swf.poll_for_activity_task(
        domain=self.domain,
        taskList={'name': self.task_list},
        identity='worker-1')

      if 'taskToken' not in task:
        print("Poll timed out, no new task.  Repoll")

      else:
        print("New task arrived: %s" % json.dumps(task))
        
        activity_name = task["activityType"]["name"]
        
        if activity_name in self.activities:
          activity = self.activities[activity_name]
          print("** Starting activity task: %s" % activity_name)
          if activity.do_activity(task):
            print("++ Activity task completed: %s" % activity_name)
            swf.respond_activity_task_completed(
                taskToken=task['taskToken'],
                result=activity.results
            )
            
            # if this is the final activity, stop polling.
            if activity_name == 'SendResultActivity':
               return True
          else:
            print("-- Activity task failed: %s" % activity_name)
            swf.respond_activity_task_failed(
                taskToken=task['taskToken'],
                reason=activity.results["reason"],
                details=activity.results["detail"]
            )
        else:
          print("couldn't find key in @activities list: %s" % activity_name)          

# if the file was run from the command-line, instantiate the class and begin the
# activities
if __name__ == "__main__":
  worker = ActivitiesWorker(init_domain(), sys.argv[1])
  worker.poll_for_activities()
  print("All done!")