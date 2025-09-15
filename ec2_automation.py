import boto3
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration - Update these values
AMI_ID = "ami-089f9ae901943684b"  # Your custom AMI ID
INSTANCE_TYPE = "t3.micro"
SECURITY_GROUP_ID = "sg-06b264a145fb5db5c"  # Your security group
REGION = "us-east-1"
SUBNET_ID = "subnet-03b096eabdbc4fe4e"  # Your subnet
S3_BUCKET = "scientiflow-bucket"
INSTANCE_PROFILE_NAME = "EC2-SSM-S3-Profile"

# Scientiflow token from environment
SCIENTIFLOW_TOKEN = os.getenv("SCIENTIFLOW_TOKEN_CONTENT")

if not SCIENTIFLOW_TOKEN:
    raise ValueError("SCIENTIFLOW_TOKEN_CONTENT not found in environment variables")

# AWS clients
ec2 = boto3.resource("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


class ScientifowAutomation:
    def __init__(self):
        self.instance_id = None
        self.commands = {}
    
    def launch_instance(self):
        """Launch EC2 instance with minimal user data."""
        print("🚀 Launching EC2 instance...")
        
        # Minimal user data - just ensure SSH keys are regenerated
        user_data_script = """#!/bin/bash
        # Regenerate SSH host keys
        ssh-keygen -A
        systemctl restart ssh

        # Ensure SSM agent is running
        snap start amazon-ssm-agent
        snap enable amazon-ssm-agent

        echo "Instance setup completed at $(date)" >> /var/log/user-data.log
        """
        
        try:
            instances = ec2.create_instances(
                ImageId=AMI_ID,
                InstanceType=INSTANCE_TYPE,
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[SECURITY_GROUP_ID],
                SubnetId=SUBNET_ID,
                IamInstanceProfile={"Name": INSTANCE_PROFILE_NAME},
                UserData=user_data_script,
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {'Key': 'Name', 'Value': f'Scientiflow-Auto-{int(time.time())}'},
                            {'Key': 'Purpose', 'Value': 'Automation'},
                            {'Key': 'AutoTerminate', 'Value': 'true'}
                        ]
                    }
                ]
            )
            
            instance = instances[0]
            self.instance_id = instance.id
            
            print(f"Instance {self.instance_id} launching...")
            instance.wait_until_running()
            instance.reload()
            
            print(f"✅ Instance {self.instance_id} is running")
            print(f"   Private IP: {instance.private_ip_address}")
            print(f"   Public IP: {instance.public_ip_address or 'None'}")
            
            # Give instance time to complete user data and register with SSM
            print("⏳ Waiting for instance to fully initialize...")
            time.sleep(30)
            
            return True
            
        except Exception as e:
            print(f"❌ Error launching instance: {e}")
            return False
    
    def wait_for_ssm_registration(self, timeout=300):
        """Wait for instance to register with SSM."""
        print(f"⏳ Waiting for SSM registration (timeout: {timeout}s)...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = ssm.describe_instance_information(
                    Filters=[
                        {
                            "Key": "InstanceIds",
                            "Values": [self.instance_id]
                        }
                    ]
                )
                
                if response["InstanceInformationList"]:
                    instance_info = response["InstanceInformationList"][0]
                    ping_status = instance_info.get("PingStatus", "Unknown")
                    
                    if ping_status == "Online":
                        print(f"✅ Instance registered with SSM (Status: {ping_status})")
                        return True
                    else:
                        print(f"⏳ SSM Status: {ping_status}, waiting...")
                
            except Exception as e:
                print(f"⚠️  Error checking SSM: {e}")
            
            time.sleep(10)
        
        print(f"❌ Instance not registered with SSM within {timeout} seconds")
        return False
    
    def send_command(self, commands, command_name, timeout=300):
        """Send command via SSM and return command ID."""
        print(f"📨 Sending {command_name}...")
        
        try:
            response = ssm.send_command(
                InstanceIds=[self.instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": commands},
                TimeoutSeconds=timeout
            )
            
            command_id = response["Command"]["CommandId"]
            self.commands[command_name] = command_id
            print(f"✅ {command_name} sent (ID: {command_id})")
            return command_id
            
        except Exception as e:
            print(f"❌ Error sending {command_name}: {e}")
            return None
    
    def monitor_command(self, command_id, command_name):
        """Monitor command execution until completion."""
        print(f"👁️  Monitoring {command_name}...")
        
        while True:
            try:
                result = ssm.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=self.instance_id
                )
                
                status = result["Status"]
                
                if status in ["Success", "Failed", "Cancelled", "TimedOut"]:
                    stdout = result.get("StandardOutputContent", "").strip()
                    stderr = result.get("StandardErrorContent", "").strip()
                    
                    if status == "Success":
                        print(f"✅ {command_name} completed successfully")
                    else:
                        print(f"❌ {command_name} failed with status: {status}")
                    
                    if stdout:
                        print(f"📋 STDOUT:\n{stdout}\n")
                    if stderr:
                        print(f"⚠️  STDERR:\n{stderr}\n")
                    
                    return status == "Success", stdout, stderr
                
                # Command still running
                time.sleep(5)
                
            except Exception as e:
                print(f"❌ Error monitoring {command_name}: {e}")
                return False, "", str(e)
    
    def run_environment_check(self):
        """Check the environment setup."""
        commands = [
            "echo '=== Environment Check ==='",
            "echo 'Current user:' $(whoami)",
            "echo 'Working directory:' $(pwd)",
            "echo 'PATH:' $PATH",
            "",
            "echo '=== Tool Verification ==='",
            "aws --version 2>&1 || echo 'AWS CLI: NOT FOUND'",
            "singularity --version 2>&1 || echo 'Singularity: NOT FOUND'",
            "scientiflow-cli --help 2>&1 || echo 'Scientiflow CLI: NOT FOUND'",
            "",
            "echo '=== System Info ==='",
            "uname -a",
            "df -h /",
            "free -h",
            "echo '=== Environment Check Complete ==='",
        ]
        
        command_id = self.send_command(commands, "Environment Check", 120)
        if command_id:
            return self.monitor_command(command_id, "Environment Check")
        return False, "", ""
    
    def run_scientiflow_workflow(self):
        """Run the main Scientiflow workflow."""
        commands = [
            "#!/bin/bash",
            "set -e",  # Exit on any error
            "",
            "echo '=== Scientiflow Workflow Started ==='",
            "",
            "# Set environment variables",
            f"export SCIENTIFLOW_TOKEN=\"{SCIENTIFLOW_TOKEN}\"",
            "echo \"$SCIENTIFLOW_TOKEN\"",
            f"echo \"{SCIENTIFLOW_TOKEN}\"",
            "export PATH='/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin:/root/.local/bin:/usr/local/singularity/bin:$PATH'",
            "",
            "# Create working directory",
            "WORK_DIR='/root/scientiflow-work'",
            "mkdir -p $WORK_DIR",
            "cd $WORK_DIR",
            "echo 'Working directory:' $(pwd)",
            "",
            "# Login to Scientiflow",
            "echo '🔐 Logging into Scientiflow...'",
            "scientiflow-cli --login --token $SCIENTIFLOW_TOKEN",
            "",
            "# Verify login",
            "if [ -f '/root/.scientiflow/key' ]; then",
            "    echo '✅ Login successful'",
            "    chmod 600 /root/.scientiflow/key",
            "else",
            "    echo '❌ Login failed - no key file found'",
            "    exit 1",
            "fi",
            "",
            "# Set base directory",
            "echo '📁 Setting base directory...'",
            "scientiflow-cli --set-base-directory --hostname \"aws-cloud\"",
            "echo '✅ Base directory set to:' $(pwd)",
            "",
            "# List available jobs",
            "echo '📋 Listing available jobs...'",
            "scientiflow-cli --list-jobs || {",
            "    echo '⚠️  List jobs command had issues, but continuing...'",
            "}",
            "",
            "# Create output directory and summary",
            "mkdir -p output",
            "echo 'Scientiflow automation completed at $(date)' > output/summary.txt",
            "echo 'Hostname: $(hostname)' >> output/summary.txt",
            "echo 'Working directory: $(pwd)' >> output/summary.txt",
            "",
            "echo '=== Scientiflow Workflow Completed ==='",
        ]
        
        command_id = self.send_command(commands, "Scientiflow Workflow", 600)
        if command_id:
            return self.monitor_command(command_id, "Scientiflow Workflow")
        return False, "", ""
    
    def upload_results_to_s3(self):
        """Upload results to S3."""
        commands = [
            "#!/bin/bash",
            "set -e",
            "",
            "echo '=== S3 Upload Started ==='",
            "",
            "# Set working directory",
            "cd /root/scientiflow-work",
            "",
            "# Test S3 access",
            f"echo '🔍 Testing S3 bucket access...'",
            f"aws s3 ls s3://{S3_BUCKET}/ || {{",
            f"    echo '❌ Cannot access S3 bucket {S3_BUCKET}'",
            "    exit 1",
            "}",
            f"echo '✅ S3 bucket {S3_BUCKET} is accessible'",
            "",
            "# Create timestamp for this run",
            "TIMESTAMP=$(date +%Y%m%d_%H%M%S)",
            "echo 'Upload timestamp:' $TIMESTAMP",
            "",
            "# Upload scientiflow directory",
            "touch testupload.py",
            f"aws s3 cp testupload.py s3://{S3_BUCKET}/scientiflow_workflows/testupload_$TIMESTAMP.py ||",
            "echo '=== S3 Upload Completed ==='",
        ]
        
        command_id = self.send_command(commands, "S3 Upload", 300)
        if command_id:
            return self.monitor_command(command_id, "S3 Upload")
        return False, "", ""
    
    def terminate_instance(self):
        """Terminate the EC2 instance."""
        if not self.instance_id:
            print("⚠️  No instance to terminate")
            return
        
        print(f"🔥 Terminating instance {self.instance_id}...")
        
        try:
            instance = ec2.Instance(self.instance_id)
            instance.terminate()
            
            print("⏳ Waiting for termination...")
            instance.wait_until_terminated()
            print(f"✅ Instance {self.instance_id} terminated successfully")
            
        except Exception as e:
            print(f"❌ Error terminating instance: {e}")
    
    def run_full_automation(self):
        """Run the complete automation workflow."""
        results = {
            "start_time": time.time(),
            "instance_id": None,
            "commands": {},
            "success": False,
            "error": None
        }
        
        try:
            print("🚀 Starting Scientiflow EC2 Automation...")
            
            # Launch instance
            if not self.launch_instance():
                raise Exception("Failed to launch instance")
            
            results["instance_id"] = self.instance_id
            
            # Wait for SSM registration
            if not self.wait_for_ssm_registration():
                raise Exception("Instance failed to register with SSM")
            
            # Run environment check
            env_success, env_stdout, env_stderr = self.run_environment_check()
            results["commands"]["environment_check"] = {
                "success": env_success,
                "stdout": env_stdout[:1000] if env_stdout else "",  # Limit output size
                "stderr": env_stderr[:1000] if env_stderr else ""
            }
            
            if not env_success:
                raise Exception("Environment check failed")
            
            # Run Scientiflow workflow
            workflow_success, workflow_stdout, workflow_stderr = self.run_scientiflow_workflow()
            results["commands"]["scientiflow_workflow"] = {
                "success": workflow_success,
                "stdout": workflow_stdout[:1000] if workflow_stdout else "",
                "stderr": workflow_stderr[:1000] if workflow_stderr else ""
            }
            
            # Upload to S3 (continue even if workflow failed)
            s3_success, s3_stdout, s3_stderr = self.upload_results_to_s3()
            results["commands"]["s3_upload"] = {
                "success": s3_success,
                "stdout": s3_stdout[:1000] if s3_stdout else "",
                "stderr": s3_stderr[:1000] if s3_stderr else ""
            }
            
            # Mark as successful if workflow succeeded
            results["success"] = workflow_success
            
            print("🎉 Automation workflow completed!")
            
        except Exception as e:
            print(f"❌ Automation failed: {e}")
            results["error"] = str(e)
            
        finally:
            # Always terminate instance
            self.terminate_instance()
            results["end_time"] = time.time()
            results["duration"] = results["end_time"] - results["start_time"]
            
        return results


def main():
    """Main function to run the automation."""
    automation = ScientifowAutomation()
    
    print("="*60)
    print("🧪 SCIENTIFLOW EC2 AUTOMATION")
    print("="*60)
    
    results = automation.run_full_automation()
    
    print("\n" + "="*60)
    print("📊 AUTOMATION SUMMARY")
    print("="*60)
    print(f"Duration: {results['duration']:.1f} seconds")
    print(f"Instance ID: {results.get('instance_id', 'None')}")
    print(f"Overall Success: {results['success']}")
    
    if results.get('error'):
        print(f"Error: {results['error']}")
    
    for cmd_name, cmd_result in results.get('commands', {}).items():
        print(f"{cmd_name}: {'✅ SUCCESS' if cmd_result['success'] else '❌ FAILED'}")
    
    print("="*60)
    
    # Save detailed results to file
    with open(f"automation_results_{int(time.time())}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    return results['success']


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)