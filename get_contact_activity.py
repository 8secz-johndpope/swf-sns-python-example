import json
from basic_activity import BasicActivity

#
# **GetContactActivity** provides a prompt for the user to enter contact
# information. When the user successfully enters contact information, the
# activity is complete.
#
# This activity returns the following results, in YAML text format:
#
# * on success: { :email => *email*, :sms => *sms* }
#
# Either value can be nil or an empty string.
#
class GetContactActivity(BasicActivity):

  # initialize the activity
  def __init__(self):
    super(GetContactActivity, self).__init__('GetContactActivity', 'v1')

  # Get some data to use to subscribe to the topic.
  def do_activity(self, task):
    print ("")
    print ("Please enter either an email address or SMS message (mobile phone) number to")
    print ("receive SNS notifications. You can also enter both to use both address types.")
    print ("")
    print ("If you enter a phone number, it must be able to receive SMS messages, and must")
    print ("be 11 digits (such as 12345678901 to represent the number 1-234-567-8901).")

    input_confirmed = False
    while input_confirmed is False:
      print ("")
      print ("Email: ")
      email = input().strip()

      print ("Phone: ")
      phone = input().strip()

      print ("")
      if (email == '') and (phone == ''):
        print ("You provided no subscription information. Quit? (y/n)")
        confirmation = input.strip().lower()
        if confirmation == 'y':
          return False
      else:
        print ("You entered:")
        print ("  email: %s" % email)
        print ("  phone: %s" % phone)
        print ("\nIs this correct? (y/n): ")
        confirmation = input().strip().lower()
        if confirmation == 'y':
          input_confirmed = True

    # make sure that @results is a single string. YAML makes this easy.
    self.results = json.dumps({ "email": email, "sms": phone })

    return True