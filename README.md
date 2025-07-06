# About 
This create two custom CRD with an operator that act on those CRDs
Inspiration: [echooperator](https://github.com/mmontes11/echoperator?tab=readme-ov-file)

# CRD 
- `echo` crd
- `scheduledEcho` crd

## Example use cases

Hello world
- Client creates a hello world Echo CRD.
- Operator receives a Echo added event.
- Operator reads the message property from the Echo and creates a Job resource.
- The Job resource creates a Pod that performs a echo command with the message property.

Scheduled hello world
- Client creates a hello world ScheduledEcho CRD.
- Operator receives a ScheduledEcho added event.
- Operator reads the message and schedule property from the ScheduledEcho and creates a CronJob.
- The CronJob schedules a Job creation using the schedule property.
- When scheduled, the Job resource creates a Pod that performs a echo command with the message property.
