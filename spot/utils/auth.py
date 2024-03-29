# Copyright 2020 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pycognito.aws_srp import AWSSRP
from requests_aws4auth import AWS4Auth

logger = logging.getLogger(__name__)

def auth_config(conf):
    if not conf.auth_type:
        logger.warning("No AuthType found, proceeding without Elasticsearch authentication")
        return

    if conf.auth_type == "direct":
        logger.debug("AuthType of 'direct' found")
        return (conf.elastic_username, conf.elastic_password)

    if conf.auth_type == "cognito":
        logger.debug("AuthType of 'cognito' found")
        # Retrieve IdToken based on username & password
        client = boto3.client('cognito-idp',
                               config=Config(signature_version=UNSIGNED,
                                             region_name=conf.cognito_region))
        aws = AWSSRP(username=conf.oauth_username,
                     password=conf.oauth_password,
                     pool_id=conf.user_pool_id,
                     client_id=conf.client_id,
                     client_secret=conf.client_secret,
                     client=client)
        token = aws.authenticate_user()
        auth_token = token["AuthenticationResult"]["IdToken"]
        logger.debug("Auth successful via cognito")

        client = boto3.client('cognito-identity', conf.cognito_region)
        # Retrieve Identity Pool ID based on IdToken
        IdRes = client.get_id(AccountId=conf.aws_account_id,
                              IdentityPoolId=conf.identity_pool_id,
                              Logins={'cognito-idp.{0}.amazonaws.com/{1}'.format(conf.cognito_region,
                                                                                 conf.user_pool_id): auth_token})

        logger.debug("Identity Pool ID retrieved")
        # Retrieve Access key and Secret access key for the retrieved Identity Pool ID
        AccessRes = client.get_credentials_for_identity(
            IdentityId=IdRes['IdentityId'],
            Logins={'cognito-idp.{0}.amazonaws.com/{1}'.format(conf.cognito_region, conf.user_pool_id): auth_token},
                    CustomRoleArn="arn:aws:iam::{0}:role/{1}".format(conf.aws_account_id, conf.elasticsearch_role_name))
        logger.debug("Access tokens retrieved")
        # Configure signing auth
        credentials = AccessRes['Credentials']
        http_auth = AWS4Auth(credentials['AccessKeyId'],
                            credentials['SecretKey'],
                            conf.elasticsearch_region, 'es',
                            session_token=credentials['SessionToken'])
        return http_auth
