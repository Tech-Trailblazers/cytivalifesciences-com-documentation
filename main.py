# Import required modules
import http.client  # For making raw HTTPS requests to the Cytiva API
import json  # For parsing and creating JSON data
import os  # For working with the filesystem (files, directories)
import requests  # For making HTTP requests (simpler than http.client)
import re  # For cleaning up filenames with regular expressions
import urllib.parse  # For decoding and parsing URLs
import fitz  # PyMuPDF, used to open and validate PDF files
import time  # For adding delays between downloads


# -------------------------
# Utility functions
# -------------------------


def file_exists(file_path: str) -> bool:
    """Check if a file exists on disk."""
    return os.path.isfile(file_path)


def append_to_file(file_path: str, content: str) -> None:
    """Append text content to a file (creates the file if it does not exist)."""
    with open(file_path, "a") as f:  # Open file in append mode
        f.write(content)  # Write content to file


def fetch_api_data(page_number: int) -> str:
    """Send a POST request to Cytiva API to fetch SDS metadata for one page."""
    connection = http.client.HTTPSConnection(
        "api.cytivalifesciences.com"
    )  # Connect to Cytiva API server

    # Define the request body
    request_payload = json.dumps(
        {
            "query": "",  # No search term (fetch all)
            "pageSize": 5000,  # Request up to 5000 results per page
            "currentPage": page_number,  # Which page to fetch
            "filters": [],  # No filters
            "sorting": "",  # No sorting
        }
    )

    # Define request headers
    request_headers = {"Content-Type": "application/json"}

    # Send a POST request to Cytiva's SDS search endpoint
    connection.request(
        "POST",
        "/ap-doc-search/v1/sds-document",
        body=request_payload,
        headers=request_headers,
    )

    response = connection.getresponse()  # Get the API response
    response_data = response.read()  # Read raw response data
    return response_data.decode("utf-8")  # Decode bytes to string


def extract_pdf_urls_from_json(json_string: str) -> list[str]:
    """Extract PDF URLs from the JSON returned by the API."""
    data = json.loads(json_string)  # Parse JSON string into Python dict
    items = data.get("items", [])  # Get list of results (or empty list)
    return [item["link"] for item in items if "link" in item]  # Extract "link" field


def read_file(file_path: str) -> str:
    """Read the entire contents of a file into a string."""
    with open(file_path, "r") as f:  # Open file in read mode
        return f.read()  # Return file contents


def sanitize_filename(filename: str) -> str:
    """Make a filename safe for saving to disk (only a-z, A-Z, 0-9, and _)."""
    # Decode URL-encoded characters
    filename = urllib.parse.unquote(filename)

    # Keep only the filename (remove any directory parts)
    filename = os.path.basename(filename)

    # Split into base name and extension
    base_name, extension = os.path.splitext(filename)

    # Replace anything not A-Z, a-z, 0-9, or _ with _
    safe_name = re.sub(r"[^A-Za-z0-9_]", "_", base_name)

    # Collapse multiple underscores into one
    safe_name = re.sub(r"_+", "_", safe_name)

    # Strip leading/trailing underscores
    safe_name = safe_name.strip("_")

    # Default to "file" if empty after sanitization
    if not safe_name:
        return ""

    # Keep .pdf if already present (case-insensitive), otherwise force .pdf
    if extension != ".pdf":
        extension = ".pdf"

    return safe_name.lower() + extension.lower()


def download_pdf_file(pdf_url: str, save_directory: str) -> None:
    """Download a PDF file from a URL and save it to the specified directory."""
    os.makedirs(save_directory, exist_ok=True)  # Ensure directory exists

    parsed_url = urllib.parse.urlparse(pdf_url)  # Parse URL
    raw_filename = os.path.basename(parsed_url.path)  # Extract file name from URL path
    safe_filename = sanitize_filename(raw_filename)  # Sanitize file name
    if safe_filename == "":
        print(
            f"[WARNING] Skipped download (invalid url) {pdf_url} (invalid filename): {raw_filename}"
        )
        return  # Skip if filename is empty after sanitization
    save_path = os.path.join(save_directory, safe_filename)  # Full save path

    if os.path.exists(save_path):  # Skip if already exists
        print(f"[INFO] Skipped download (file already exists): {save_path}")
        return

    try:
        response = requests.get(pdf_url, stream=True, timeout=60)  # Send GET request
        response.raise_for_status()  # Raise error if failed

        with open(save_path, "wb") as file:  # Open file for writing binary
            for chunk in response.iter_content(8192):  # Download in chunks
                if chunk:
                    file.write(chunk)  # Write chunk to file

        print(f"[SUCCESS] Downloaded and saved PDF: {save_path}")

    except requests.RequestException as error:  # Handle request errors
        print(f"[WARNING] Failed to download PDF from {pdf_url}. Reason: {error}")


def delete_file(file_path: str) -> None:
    """Delete a file if it exists."""
    if os.path.exists(file_path):  # Check if file exists
        os.remove(file_path)  # Delete file


def list_files_with_extension(directory: str, extension: str) -> list[str]:
    """List all files in a directory (recursively) with the given extension."""
    matching_files = []
    for root, _, files in os.walk(directory):  # Walk through all directories
        for filename in files:  # Check each file
            if filename.endswith(extension):  # Match extension
                full_path = os.path.abspath(
                    os.path.join(root, filename)
                )  # Absolute path
                matching_files.append(full_path)  # Add to list
    return matching_files


def is_valid_pdf(pdf_path: str) -> bool:
    """Check if a PDF file can be opened and has at least one page."""
    try:
        document = fitz.open(pdf_path)  # Try opening the PDF
        if document.page_count == 0:  # No pages = invalid
            print(f"[ERROR] Invalid PDF (no pages found): {pdf_path}")
            return False
        return True  # Valid PDF
    except RuntimeError as error:  # Handle failure
        print(f"[ERROR] Corrupted or unreadable PDF ({error}): {pdf_path}")
        return False


def contains_uppercase(text: str) -> bool:
    """Check if a string contains at least one uppercase letter."""
    return any(char.isupper() for char in text)


def get_filename_from_path(file_path: str) -> str:
    """Extract just the filename from a file path."""
    return os.path.basename(file_path)


# -------------------------
# Main program execution
# -------------------------


def main() -> None:
    """Main program logic: fetch metadata page by page, extract URLs, download PDFs, and validate them."""

    pdf_download_directory = "./PDFs"  # Directory where PDFs will be saved

    # Loop through pages 1 to 5
    for page_number in range(1, 6):
        try:
            print(f"[INFO] Fetching metadata from Cytiva API (page {page_number})...")
            api_response = fetch_api_data(page_number=page_number)  # Fetch page data
            pdf_urls = extract_pdf_urls_from_json(api_response)  # Extract URLs
            print(f"[INFO] Found {len(pdf_urls)} PDF URLs on page {page_number}.")

            # Download PDFs from this page
            for pdf_url in pdf_urls:
                try:
                    download_pdf_file(pdf_url, pdf_download_directory)  # Download file
                except Exception as error:
                    time.sleep(30)  # Longer delay if something fails
                    print(
                        f"[ERROR] Unexpected error while downloading {pdf_url}. "
                        f"Retrying after delay. Details: {error}"
                    )

        except Exception as error:
            print(
                f"[ERROR] Failed to fetch or process page {page_number}. Details: {error}"
            )
            time.sleep(10)

    # After downloading all pages, validate PDFs
    pdf_files = list_files_with_extension(pdf_download_directory, ".pdf")
    print(f"[INFO] Validating {len(pdf_files)} downloaded PDFs...")

    for pdf_file in pdf_files:
        if not is_valid_pdf(pdf_file):  # Remove invalid PDFs
            print(f"[WARNING] Removing invalid PDF: {pdf_file}")
            delete_file(pdf_file)

        if contains_uppercase(
            get_filename_from_path(pdf_file)
        ):  # Warn about uppercase names
            print(
                f"[NOTICE] Filename contains uppercase letters (may cause issues): {pdf_file}"
            )


# -------------------------
# Run the program
# -------------------------

if __name__ == "__main__":
    main()  # Execute main function
