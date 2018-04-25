import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
import uuid
import json

from utils import init_domain

botoConfig = Config(connect_timeout=50, read_timeout=70)
swf = boto3.client('swf', config=botoConfig)

class SampleWorkflow:

  def __init__(self, task_list):

    # the domain to look for decision tasks in.
    self.domain = init_domain()

    # the task list is used to poll for decision tasks.
    self.task_list = task_list

    # The list of activities to run, in order. These name/version hashes can be
    # passed directly to AWS::SimpleWorkflow::DecisionTask#schedule_activity_task.
    self.activity_list = [
      { 'name': 'GetContactActivity', 'version': 'v1' },
      { 'name': 'SubscribeTopicActivity', 'version': 'v1' },
      { 'name': 'WaitForConfirmationActivity', 'version': 'v1' },
      { 'name': 'SendResultActivity', 'version': 'v1' },
    ] 
    self.activity_list.reverse() # reverse the order... we're treating this like a stack.
    
    self.activity_version = '1'

    self.register_workflow()

  # Registers the workflow
  def register_workflow(self):
    self.workflow_name = 'swf-sns-workflow'
    # a default value...
    self.workflow_version = '1'

    try:
      swf.register_workflow_type(
        domain=self.domain,
        name=self.workflow_name,
        version=self.workflow_version,
        description="Sample workflow",
        defaultExecutionStartToCloseTimeout=str(3600),
        defaultTaskStartToCloseTimeout=str(24*3600),
        defaultChildPolicy="TERMINATE",
        defaultTaskList={"name": self.task_list}
      )
      print("Sample workflow created!")
    except ClientError as e:
      print("Workflow already exists: ", e.response.get("Error", {}).get("Code"))

  # poll for decision tasks
  def poll_for_decisions(self):
    print("Listening for Decision Tasks")

    while True:
      newTask = swf.poll_for_decision_task(
          domain=self.domain,
          taskList={'name': self.task_list},
          identity='decider-sample-1',
          reverseOrder=False)
          
      if 'taskToken' not in newTask:
          print("Poll timed out, no new task.  Repoll")
          
      elif 'events' in newTask:
          eventHistory = [evt for evt in newTask['events'] if not evt['eventType'].startswith('Decision')]
          lastEvent = eventHistory[-1]
          print(lastEvent)

          if lastEvent['eventType'] == 'WorkflowExecutionStarted': # and newTask['taskToken'] not in outstandingTasks:
            print(self.activity_list)
            print("Dispatching task to worker", self.activity_list[-1], newTask['workflowExecution'], newTask['workflowType'])
            swf.respond_decision_task_completed(
              taskToken=newTask['taskToken'],
              decisions=[
                {
                  'decisionType': 'ScheduleActivityTask',
                  'scheduleActivityTaskDecisionAttributes': {
                      'activityType':{
                          'name': self.activity_list[-1]["name"], # get last item in task list
                          'version': self.activity_list[-1]["version"]
                          },
                      'activityId': 'activityid-' + str(uuid.uuid4()),
                      'input': '',
                      'scheduleToCloseTimeout': 'NONE',
                      'scheduleToStartTimeout': 'NONE',
                      'startToCloseTimeout': 'NONE',
                      'heartbeatTimeout': 'NONE',
                      'taskList': {'name': self.task_list + '-activities'},
                  }
                }
              ]
            )
            print("Task Dispatched:", newTask['taskToken'])

          elif lastEvent['eventType'] == 'ActivityTaskCompleted':
            # we are running the activities in strict sequential order, and
            # using the results of the previous activity as input for the next
            # activity.
            last_activity = self.activity_list.pop()
            
            # if all activitities done
            if not self.activity_list:
              swf.respond_decision_task_completed(
                taskToken=newTask['taskToken'],
                decisions=[
                  {
                    'decisionType': 'CompleteWorkflowExecution',
                    'completeWorkflowExecutionDecisionAttributes': {
                      'result': 'success'
                    }
                  }
                ]
              )
              print("Workflow Completed!")
              return False
            else:
              print("Dispatching task to worker", self.activity_list[-1], newTask['workflowExecution'], newTask['workflowType'])
              decisions=[
                {
                  'decisionType': 'ScheduleActivityTask',
                  'scheduleActivityTaskDecisionAttributes': {
                      'activityType':{
                          'name': self.activity_list[-1]["name"], # get last item in task list
                          'version': self.activity_list[-1]["version"]
                          },
                      'activityId': 'activityid-' + str(uuid.uuid4()),
                      'input': '',
                      'scheduleToCloseTimeout': 'NONE',
                      'scheduleToStartTimeout': 'NONE',
                      'startToCloseTimeout': 'NONE',
                      'heartbeatTimeout': 'NONE',
                      'taskList': {'name': self.task_list + '-activities'},
                  }
                }
              ]
              
              if "result" in lastEvent['activityTaskCompletedEventAttributes']:
                decisions[0]['scheduleActivityTaskDecisionAttributes']['input'] = lastEvent['activityTaskCompletedEventAttributes']['result']
                
              swf.respond_decision_task_completed(
                taskToken=newTask['taskToken'],
                decisions=decisions
              )
              print("Task Dispatched:", newTask['taskToken'])
              
  def start_execution(self):
    response = swf.start_workflow_execution(
      domain=self.domain,
      workflowId='sample-1',
      workflowType={
        "name": self.workflow_name,
        "version": self.workflow_version
      },
      taskList={
          'name': self.task_list
      },
      input=''
    )
    
    self.poll_for_decisions()

# if the file was run from the command-line, instantiate the class and begin the workflow execution.
if __name__ == "__main__":

  # Get an UUID to use as the task list name. We'll use the same task list name
  # in the activity worker.

  task_list = str(uuid.uuid4())

  # Let the user start the activity worker first...

  print("")
  print( "Amazon SWF Example" )
  print( "------------------" )
  print( "" )
  print( "Start the activity worker, preferably in a separate command-line window, with" )
  print( "the following command:" )
  print( "" )
  print( "> python activities_worker.py %s-activities" % task_list ) 
  print( "" )
  print( "You can copy & paste it if you like, just don't copy the '>' character." )
  print( "" )
  print( "Press return when you're ready..." )

  i = input()

  # Now, start the workflow.

  print( "Starting workflow execution." )
  sample_workflow = SampleWorkflow(task_list)
  sample_workflow.start_execution()