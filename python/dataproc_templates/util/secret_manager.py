from google.cloud import secretmanager_v1 as secretmanager
import google.auth
import re


def access_secret_version(secret_id, version_id="latest"):
    """
    Get secret from the secret manager

    Args:
        secret_id: secret name
        version_id: latest(default)

    Returns:
        Secret value

    """
    project_id: str
    _, project_id = google.auth.default()

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    if validate_secret(secret_id):
        # Build the resource name of the secret version.
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    else:
        raise Exception("Invalid secret name. Secret name should not contain any other special symbol except - or _")

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')


def validate_secret(secret_id):
    valid_secret = True
    regexp = re.compile('[^0-9a-zA-Z_-]+')
    if regexp.search(secret_id):
        valid_secret = False
    return valid_secret
