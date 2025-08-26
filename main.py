# This is a sample Python script.
import requests


def run_pipeline():
    # В качестве жертвы у меня librarianmon
    api_url = "http://127.0.0.1:8182/users"  # Example API endpoint

    # Make the GET request
    response = requests.get(api_url)

    # Check for successful response (status code 200)
    if response.status_code == 200:
        # Parse the JSON response into a Python dictionary or list
        data = response.json()
        print("Data retrieved successfully:")
        print(data)
    else:
        print(f"Error: Unable to retrieve data. Status code: {response.status_code}")
        print(response.text)  # Print the error message if available






# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_pipeline()