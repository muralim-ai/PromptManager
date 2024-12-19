import uuid
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, ValidationError
import json
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class Prompt(BaseModel):
    id: Optional[str] = None
    datapoint: str
    clause: str
    created_by: str
    query_prompt: str
    version: Optional[int] = 1
    evaluated: Optional[int] = 0
    status: Optional[str] = "latest"
    score: Optional[dict] = None
    notes: Optional[str] = None

class PromptManager:
    def __init__(self, 
                 connection_url: str, 
                 container_name: str,
                 file_path: str = "prompts.json"):
        self.file_path = file_path
        self.prompts: List[Prompt] = self._load_prompts()

        self.connection_url = connection_url
        self.container_name = container_name

        # Use DefaultAzureCredential for AAD authentication
        self.credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(self.connection_url, credential=self.credential)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def _load_prompts(self) -> List[Prompt]:
        try:
            blob_client = self.container_client.get_blob_client(self.local_file_path)
            if blob_client.exists():
                with open(self.local_file_path, "wb") as my_blob:
                    download_stream = blob_client.download_blob()
                    my_blob.write(download_stream.readall())
                with open(self.local_file_path, "r") as f:
                    data = json.load(f)
                    return [Prompt(**prompt_data) for prompt_data in data]
            else:
                print("Blob not found. Starting with empty prompts.")
                return []

        except Exception as e:  # Catching broader exceptions for Azure-related issues
            print(f"Error loading prompts: {e}. Starting with empty prompts.")
            return []
        
    def _save_prompts(self):
        try:
            with open(self.local_file_path, "w") as f:
                json.dump([prompt.model_dump() for prompt in self.prompts], f, indent=4)

            blob_client = self.container_client.get_blob_client(self.local_file_path)
            with open(self.local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
        except Exception as e:
             print(f"Error saving prompts: {e}")

    def create_prompt(self, prompt_data: Dict[str, Any]) -> Optional[Prompt]:
        try:
            new_id = str(uuid.uuid4()) #Generate UUID
            prompt_data["id"] = new_id
            new_prompt = Prompt(**prompt_data)
            # Check if a similar prompt exists (by datapoint and clause)
            existing_prompt_index = self._find_prompt_index(new_prompt.datapoint, new_prompt.clause)
            if existing_prompt_index != -1:
                # Increment version and update status
                existing_prompt = self.prompts[existing_prompt_index]
                new_prompt.version = existing_prompt.version + 1
                new_prompt.evaluated = 0 #reset evaluation
                self.prompts[existing_prompt_index].status = "superseded"
                self.prompts.append(new_prompt)
            else:
              self.prompts.append(new_prompt)
            self._save_prompts()
            return new_prompt
        except ValidationError as e:
            print(f"Validation error creating prompt: {e}")
            return None
        
    def get_prompts(self, id: Optional[str] = None, datapoint: Optional[str] = None, clause: Optional[str] = None, status: Optional[str] = None) -> List[Prompt]:
        filtered_prompts = self.prompts
        if id:
            filtered_prompts = [p for p in filtered_prompts if p.id == id]
        if datapoint:
            filtered_prompts = [p for p in filtered_prompts if p.datapoint == datapoint]
        if clause:
            filtered_prompts = [p for p in filtered_prompts if p.clause == clause]
        if status:
            filtered_prompts = [p for p in filtered_prompts if p.status == status]
        return filtered_prompts

    def update_prompt(self, id: str, updated_data: Dict[str, Any]) -> Optional[Prompt]:
        index = self._find_prompt_index_by_id(id)
        if index != -1:
            try:
                updated_prompt = self.prompts[index].model_copy(update=updated_data)
                self.prompts[index] = updated_prompt
                self._save_prompts()
                return updated_prompt
            except ValidationError as e:
                print(f"Validation error updating prompt: {e}")
                return None
        else:
            print(f"No prompt found with id {id} to update")
            return None
    
    def _find_prompt_index(self, datapoint:str, clause:str, status: Optional[str] = None) -> int:
        for i, prompt in enumerate(self.prompts):
            if prompt.datapoint == datapoint and prompt.clause == clause:
                if status is None or prompt.status == status:
                    return i
        return -1
    
    def _find_prompt_index_by_id(self, id: str) -> int:
        for i, prompt in enumerate(self.prompts):
            if prompt.id == id:
                return i
        return -1

    def get_query_prompts_and_notes(self, datapoint: str) -> List[Dict[str, str]]:
        """
        Retrieves all query prompts and notes for a given datapoint.

        Args:
            datapoint: The datapoint to search for.

        Returns:
            A list of dictionaries, where each dictionary contains the 'query_prompt' and 'notes' 
            for a matching prompt. Returns an empty list if no matching prompts are found.
        """
        matching_prompts = [p for p in self.prompts if p.datapoint == datapoint]
        result = []
        for prompt in matching_prompts:
            result.append({
                "query_prompt": prompt.query_prompt,
                "notes": prompt.notes or "" # Provide empty string if notes is None
            })
        return result
    
    def get_prompts_info(self, search_term: str, search_field: str) -> List[Dict[str, Union[str, int, None]]]:
        """
        Retrieves prompts information (id, query_prompt, notes, version) based on a search term and field.

        Args:
            search_term: The value to search for.
            search_field: The field to search in ('id', 'datapoint', or 'clause').

        Returns:
            A list of dictionaries, where each dictionary contains 'id', 'query_prompt', 'notes', and 'version'
            for matching prompts. Returns an empty list if no matching prompts are found or if the search_field is invalid.
        """

        if search_field not in ("id", "datapoint", "clause"):
            print("Invalid search field. Must be 'id', 'datapoint', or 'clause'.")
            return []

        matching_prompts = []
        if search_field == "id":
            matching_prompts = self.get_prompts(id=search_term)
        elif search_field == "datapoint":
            matching_prompts = self.get_prompts(datapoint=search_term)
        elif search_field == "clause":
            matching_prompts = self.get_prompts(clause=search_term)

        result = []
        for prompt in matching_prompts:
            result.append({
                "id": prompt.id,
                "query_prompt": prompt.query_prompt,
                "notes": prompt.notes,
                "version": prompt.version
            })
        return result


# Example Usage
if __name__ == "__main__":
    
    # Replace with your actual connection URL and container name
    connection_url = "https://<your_storage_account_name>.blob.core.windows.net"  # Use f-string for variable substitution
    container_name = "prompt-container"

    try:
        manager = PromptManager(connection_url, container_name)
        # ... (rest of the example usage remains the same)
    except Exception as e:
        print(f"Error during setup or usage: {e}")

    #manager = PromptManager()

    new_prompt_data = {
        "datapoint": "What is the Rent?",
        "clause": "Rent",
        "created_by": "Murali",
        "query_prompt": "As a {{criticLevel}} movie critic, do you like {{movie}}?",
        "version": 1,
        "evaluated": 1,
        "status": "latest",
        "notes": "Initial prompt"
    }
    new_prompt = manager.create_prompt(new_prompt_data)

    if new_prompt:
        print("Created Prompt:", new_prompt)
        created_prompt_id = new_prompt.id #Capture the id

        updated_prompt = manager.update_prompt(created_prompt_id, {"query_prompt": "Updated Prompt"})
        if updated_prompt:
            print("Updated Prompt:", updated_prompt)
        
        found_prompt = manager.get_prompts(id=created_prompt_id)
        if found_prompt:
            print("Found prompt by ID:", found_prompt[0])
 
    new_prompt_data_v2 = {
        "datapoint": "Is base Rent required?",
        "clause": "Rent",
        "created_by": "Murali",
        "query_prompt": "What is the base rent for the property?",
        "version": 2,
        "evaluated": 1,
        "status": "latest",
        "notes": "held, maintained means that the rent remains the same"
    }
    new_prompt_v2 = manager.create_prompt(new_prompt_data_v2)
    if new_prompt_v2:
      print("Created Prompt V2:", new_prompt_v2)

    latest_prompts = manager.get_prompts(status="latest")
    print("Latest Prompts:", latest_prompts)
    all_prompts = manager.get_prompts()
    print("All Prompts:", all_prompts)

    # Example of getting query prompts, notes, id, and version by datapoint
    datapoint_to_search = "Is Security Required"
    prompts_info = manager.get_prompts_info(datapoint_to_search, "datapoint")
    if prompts_info:
        print(f"Prompts Info for Datapoint '{datapoint_to_search}':")
        for item in prompts_info:
            print(f"  ID: {item['id']}")
            print(f"  Query Prompt: {item['query_prompt']}")
            print(f"  Notes: {item['notes']}")
            print(f"  Version: {item['version']}")
    else:
        print(f"No prompts found for datapoint '{datapoint_to_search}'")


    # Example of getting query prompts, notes, id, and version by id
    if new_prompt: #Checking if new_prompt was created in previous example
        prompts_info = manager.get_prompts_info(new_prompt.id, "id")
        if prompts_info:
            print(f"Prompts Info for ID '{new_prompt.id}':")
            for item in prompts_info:
                print(f"  ID: {item['id']}")
                print(f"  Query Prompt: {item['query_prompt']}")
                print(f"  Notes: {item['notes']}")
                print(f"  Version: {item['version']}")
        else:
            print(f"No prompts found for id '{new_prompt.id}'")

    # Example of getting query prompts, notes, id, and version by clause
    clause_to_search = "Security Deposit"
    prompts_info = manager.get_prompts_info(clause_to_search, "clause")
    if prompts_info:
        print(f"Prompts Info for Clause '{clause_to_search}':")
        for item in prompts_info:
            print(f"  ID: {item['id']}")
            print(f"  Query Prompt: {item['query_prompt']}")
            print(f"  Notes: {item['notes']}")
            print(f"  Version: {item['version']}")
    else:
        print(f"No prompts found for clause '{clause_to_search}'")

    # Example of invalid search field
    prompts_info = manager.get_prompts_info("some_term", "invalid_field") #Example with invalid search field
    if prompts_info:
        print("Prompts Info:")
        for item in prompts_info:
            print(f"  ID: {item['id']}")
            print(f"  Query Prompt: {item['query_prompt']}")
            print(f"  Notes: {item['notes']}")
            print(f"  Version: {item['version']}")
    else:
        print("No prompts found")    