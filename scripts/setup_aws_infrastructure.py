"""
Script to set up initial AWS infrastructure
"""
import boto3
import json
import time
from botocore.exceptions import ClientError

class AWSInfrastructureSetup:
    def __init__(self, environment='dev'):
        self.environment = environment
        self.region = 'us-east-1'
        
        # Initialize AWS clients
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.ecr_client = boto3.client('ecr', region_name=self.region)
        self.iam_client = boto3.client('iam', region_name=self.region)
        self.dynamodb_client = boto3.client('dynamodb', region_name=self.region)
    
    def create_ecr_repositories(self):
        """Create ECR repositories for container images"""
        repositories = [
            'ecommerce-pipeline-validation',
            'ecommerce-pipeline-transformation'
        ]
        
        for repo_name in repositories:
            try:
                response = self.ecr_client.create_repository(
                    repositoryName=repo_name,
                    imageScanningConfiguration={'scanOnPush': True},
                    encryptionConfiguration={'encryptionType': 'AES256'}
                )
                print(f"✅ Created ECR repository: {repo_name}")
                print(f"   URI: {response['repository']['repositoryUri']}")
                
                # Set lifecycle policy to keep only latest 10 images
                lifecycle_policy = {
                    "rules": [
                        {
                            "rulePriority": 1,
                            "description": "Keep last 10 images",
                            "selection": {
                                "tagStatus": "any",
                                "countType": "imageCountMoreThan",
                                "countNumber": 10
                            },
                            "action": {
                                "type": "expire"
                            }
                        }
                    ]
                }
                
                self.ecr_client.put_lifecycle_policy(
                    repositoryName=repo_name,
                    lifecyclePolicyText=json.dumps(lifecycle_policy)
                )
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
                    print(f"ℹ️  ECR repository already exists: {repo_name}")
                else:
                    print(f"❌ Error creating ECR repository {repo_name}: {e}")
    
    def create_s3_buckets(self):
        """Create S3 buckets for the pipeline"""
        buckets = [
            f'ecommerce-raw-data-{self.environment}',
            f'ecommerce-processed-data-{self.environment}',
            f'ecommerce-failed-data-{self.environment}',
            f'ecommerce-terraform-state-{self.environment}'
        ]
        
        for bucket_name in buckets:
            try:
                if self.region == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=bucket_name)
                else:
                    self.s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                
                # Enable versioning
                self.s3_client.put_bucket_versioning(
                    Bucket=bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                
                # Enable server-side encryption
                self.s3_client.put_bucket_encryption(
                    Bucket=bucket_name,
                    ServerSideEncryptionConfiguration={
                        'Rules': [
                            {
                                'ApplyServerSideEncryptionByDefault': {
                                    'SSEAlgorithm': 'AES256'
                                }
                            }
                        ]
                    }
                )
                
                # Block public access
                self.s3_client.put_public_access_block(
                    Bucket=bucket_name,
                    PublicAccessBlockConfiguration={
                        'BlockPublicAcls': True,
                        'IgnorePublicAcls': True,
                        'BlockPublicPolicy': True,
                        'RestrictPublicBuckets': True
                    }
                )
                
                print(f"✅ Created S3 bucket: {bucket_name}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                    print(f"ℹ️  S3 bucket already exists: {bucket_name}")
                else:
                    print(f"❌ Error creating S3 bucket {bucket_name}: {e}")
    
    def create_iam_roles(self):
        """Create IAM roles for ECS tasks"""
        
        # ECS Task Execution Role
        execution_role_name = f'ecommerce-ecs-execution-role-{self.environment}'
        execution_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ecs-tasks.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        try:
            self.iam_client.create_role(
                RoleName=execution_role_name,
                AssumeRolePolicyDocument=json.dumps(execution_trust_policy),
                Description=f'ECS task execution role for {self.environment}'
            )
            
            # Attach managed policy
            self.iam_client.attach_role_policy(
                RoleName=execution_role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy'
            )
            
            print(f"✅ Created ECS execution role: {execution_role_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"ℹ️  ECS execution role already exists: {execution_role_name}")
            else:
                print(f"❌ Error creating ECS execution role: {e}")
        
        # ECS Task Role
        task_role_name = f'ecommerce-ecs-task-role-{self.environment}'
        task_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::ecommerce-*-{self.environment}",
                        f"arn:aws:s3:::ecommerce-*-{self.environment}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:Query",
                        "dynamodb:Scan"
                    ],
                    "Resource": [
                        f"arn:aws:dynamodb:{self.region}:*:table/*-{self.environment}"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        try:
            self.iam_client.create_role(
                RoleName=task_role_name,
                AssumeRolePolicyDocument=json.dumps(execution_trust_policy),
                Description=f'ECS task role for {self.environment}'
            )
            
            # Create and attach inline policy
            self.iam_client.put_role_policy(
                RoleName=task_role_name,
                PolicyName=f'ecommerce-task-policy-{self.environment}',
                PolicyDocument=json.dumps(task_policy)
            )
            
            print(f"✅ Created ECS task role: {task_role_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"ℹ️  ECS task role already exists: {task_role_name}")
            else:
                print(f"❌ Error creating ECS task role: {e}")
    
    def setup_all(self):
        """Set up all AWS infrastructure"""
        print(f"🚀 Setting up AWS infrastructure for {self.environment} environment...")
        print("=" * 60)
        
        self.create_ecr_repositories()
        print()
        
        self.create_s3_buckets()
        print()
        
        self.create_iam_roles()
        print()
        
        print("✅ AWS infrastructure setup completed!")
        print("\nNext steps:")
        print("1. Run Terraform to create VPC and other resources")
        print("2. Build and push container images to ECR")
        print("3. Deploy ECS services")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Set up AWS infrastructure')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'])
    
    args = parser.parse_args()
    
    setup = AWSInfrastructureSetup(args.environment)
    setup.setup_all()

if __name__ == "__main__":
    main()