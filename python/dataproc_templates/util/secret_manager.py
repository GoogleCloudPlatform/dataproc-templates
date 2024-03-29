from google.cloud import secretmanager
import google.auth

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

    secret_id = sanitize_secret(secret_id)
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')


def sanitize_secret(secret_id):
    secret_id = secret_id.replace('{', '').replace('}', '')
    return secret_id