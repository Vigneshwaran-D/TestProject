from flask import Flask, jsonify, request
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from pandasai import SmartDatalake
from pandasai.llm import OpenAI
from langchain.chat_models import AzureChatOpenAI
from dotenv import load_dotenv
from datetime import datetime
import uuid
import shutil
from azure.storage.blob import ContainerClient 
from pandasai import SmartDataframe
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta


# Load the environment variables
load_dotenv()

app = Flask(__name__)

def generate_unique_filename():
    """
    Generate a unique filename based on current timestamp and a random UUID.
    
    Returns:
    - A string representing the unique file path.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:6]  # get first 6 characters of UUID for brevity
    return f"./exports/charts/chart_{timestamp}_{unique_id}.png"


class AzureBlobUploader:
    def __init__(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    def upload_to_container(self, container_name, file_path):
        """
        Uploads a file to a given Azure Blob Storage container.
        
        Args:
        - container_name: The name of the Azure Blob Storage container.
        - file_path: The path to the file you wish to upload.
        
        Returns:
        - The URL of the uploaded blob.
        """
        blob_name = os.path.basename(file_path)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        with open(file_path, 'rb') as file:
            blob_client.upload_blob(file)
        
        return blob_client.url

class AzureBlobUploaderWithSAS:
    def __init__(self, account_name, account_key, container_name):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name

    def generate_sas_token(self):
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(create=True, write=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        return sas_token

    def upload_to_container(self, file_path):
        sas_token = self.generate_sas_token()
        blob_name = os.path.basename(file_path)
        
        # Create a BlobClient using the generated SAS token
        blob_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{blob_name}"
        blob_client = BlobClient.from_blob_url(blob_url=blob_url, credential=sas_token)
        
        with open(file_path, 'rb') as file:
            blob_client.upload_blob(file)

        return blob_client.url

employees_df = pd.DataFrame(
    {
        "EmployeeID": [1, 2, 3, 4, 5],
        "Name": ["John", "Emma", "Liam", "Olivia", "William"],
        "Department": ["HR", "Sales", "IT", "Marketing", "Finance"],
    }
)


salaries_df = pd.DataFrame(
    {
        "EmployeeID": [1, 2, 3, 4, 5],
        "Salary": [5000, 6000, 4500, 7000, 5500],
    }
)


llmtest = AzureChatOpenAI(
            deployment_name=os.getenv("OPENAI_DEPLOYMENT_NAME"),
            openai_api_base=os.getenv("OPENAI_DEPLOYMENT_ENDPOINT"),
            openai_api_version=os.getenv("OPENAI_DEPLOYMENT_VERSION"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_api_type="azure"
        )


dl = SmartDatalake(
    [employees_df, salaries_df],
    config={
        "llm": llmtest,
        "verbose": True,
    },
)


@app.route('/ask', methods=['POST'])
def ask_question():
    data = request.get_json()

    if not data or 'user_input' not in data:
        return jsonify({"error": "user_input is required"}), 400

    user_input = data['user_input']
    response_text = dl.chat(user_input)
    
    response = {}
    
    if response_text:
        response["answer"] = response_text
    else:
        unique_file_path = generate_unique_filename()
        shutil.copy('./exports/charts/temp_chart.png', unique_file_path)

        CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=vigneshblobs;AccountKey=nO6aQnH0201HpEdwmyu9vp1NkkX4D21NWHF/dyEx08pcAQW43rfBMa7C3IgfF5ZR7i83hg4dZqUZ+AStY3ueag==;EndpointSuffix=core.windows.net"
        uploader = AzureBlobUploader(CONNECTION_STRING)

        CONTAINER_NAME = "sai"
        
        uploaded_url = uploader.upload_to_container(CONTAINER_NAME, unique_file_path)
        response["answer"] = uploaded_url

    #return jsonify(response)
    return response


if __name__ == "__main__":
    app.run(debug=False)
