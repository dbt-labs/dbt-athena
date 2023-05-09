AWS advertise a method for using Athena for "Security conscious customers". ([announcement](https://aws.amazon.com/about-aws/whats-new/2020/03/amazon-athena-support-querying-data-s3-buckets-aws-iam-condition-key/), [reference doc](https://docs.aws.amazon.com/athena/latest/ug/security-iam-athena-calledvia.html)). `dbt-athena` currently has not support this way of working since v1.3.5 as it requires direct access to Glue and S3 APIs. It may support this more restricted operation in the future, and this doc is start!

[restrictive_role_cf.yaml]() is a CloudFormation template that creates a role `DbtAthenaRestricted` in an AWS account. CloudFormation is used to make updates easy and avoid scripting or depending on manual actions, but you could translate the policy into JSON and copy-paste into IAM. It can be used to understand the effective restrictions and to bootstrap a testing solution for this mode of operation.

- **Before using it, you should ensure you understand and accept the role and assume-role policy as appropriate for your target AWS account.**
- The role may be updated in the future, but should not be extended to assume broader permissions without consideration of impact to users.

## Usage

The following assumes:
- you are authenaticated to an appropriate AWS account for testing with privileges to create/update/delete CloudFormation stacks and to create/update/delete IAM roles.
- you are running from the repo's root directory
- you are runnins in a bash session
- your AWS CLI configuration is standard


### Create Stack

`aws cloudformation create-stack --stack-name dbt-testing --capabilities CAPABILITY_NAMED_IAM --template-body file://tests/restrictive_role_cf.yaml`

### Update Stack

Same as above, but `update-stack` instead of `create-stack`

### Assuming the Role

Update `~/.aws/config` to add a profile using the new role:

```
[profile dbt-restricted]
role_arn = arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/DbtAthenaRestricted
source_profile = default (or a named profile that can assume this role)
```

### Using the Role

Example of using the role with AWS CLI to run a query and check results

```bash
QUERY_EXECUTION_ID=$(aws athena start-query-execution --query-string "SHOW DATABASES" --profile dbt-restricted --work-group primary --query QueryExecutionId --output text)`
aws athena get-query-results --query-execution-id "${QUERY_EXECUTION_ID}" --profile dbt-restricted
```
