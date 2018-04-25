from utils import init_domain
import boto3
import botocore

# Base for activities in the SWF+SNS sample.
class BasicActivity:
  # Initializes a BasicActivity.
  #
  # @param [String] name
  #   The activity's name, which will be used to identify the activity to SWF
  #   and to other parts of the workflow.
  #
  # @param [String] version
  #   The version string for the activity.
  #
  # @param [AWS::SimpleWorkflow::ActivityOptions] options
  #   The options for the ActivityType that will be registered when the
  #   BasicActivity is initialized.
  #
  def __init__(self, name = 'basic_activity', version = 'v1', options = None):
    self.activity_type = None
    self.name = name
    self.results = None

    # If no options were specified, use some reasonable defaults.
    if options == None:
      options = {
        # All timeouts are in seconds.
        "default_task_heartbeat_timeout": 900,
        "default_task_schedule_to_start_timeout": 120,
        "default_task_schedule_to_close_timeout": 3800,
        "default_task_start_to_close_timeout": 3600 }

    # get the domain to use for activity tasks.
    self.domain = init_domain()
    
    swf = boto3.client('swf')
    
    try:
      swf.register_activity_type(
        domain=self.domain,
        name=self.name,
        version=version,
        description="Sample worker",
        defaultTaskStartToCloseTimeout=str(options["default_task_start_to_close_timeout"])
        #defaultTaskList={"name": TASKLIST}
      )
      print("Test worker created!")
    except botocore.exceptions.ClientError as e:
      print("Activity already exists: ", e.response.get("Error", {}).get("Code"))

  # Performs tha activity.
  #
  # Usually called by the activity task poller, this method should always set a
  # value for `@results`, which must be a string. The contents of `@results`
  # will be submitted to the next activity in the sequence by SWF.
  #
  # The base class version of this is *meant* to be overridden, and just copies
  # the input to the results.
  #
  # @param [String] input
  #   The input to the activity, as passed in through the activity task.
  #
  # @return [Boolean]
  #   `true` if the activity completed successfully, or `false` if it failed.
  #
  def do_activity(self, task):
    self.results = task["input"] # may be nil
    return True