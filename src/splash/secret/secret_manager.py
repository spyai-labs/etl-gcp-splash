from google.cloud import secretmanager  # type: ignore


__all__ = [
    'SecretDataType',
    'add_version',
    'add_and_destroy_prev',
    'get_version',
    'get_version_info',
    'destroy_version'
]

SecretDataType = str|int|float|bool|bytes


def add_version(
    project_id: str, secret_id: str, data: SecretDataType, mute: bool = True
) -> secretmanager.SecretVersion:
    """
    Adds a new secret version to the given secret with the provided data. 
    Args:
        project_id: the id of the GCP project where the secret is stored
        secret_id: the id of the secret to add a new version for
        data: data to store in the secret manager
        mute: option to silence the success feedback message. (default True)
    Returns:
        response: secretmanager.SecretVersion
    """
    if not isinstance(data, SecretDataType): # type: ignore
        raise TypeError(f"Unsupported secret data type. First convert the data into one of the following dtype: {{{SecretDataType}}} ")
    
    try:
        client = secretmanager.SecretManagerServiceClient()
        parent = client.secret_path(project_id, secret_id)
        
        response = client.add_secret_version(
            request={
                "parent": parent, 
                "payload": {"data": str(data).encode("UTF-8")}
            }
        )
        
        if not mute:
            print(f"Added a new secret version with name: '{response.name}'")
        
    except Exception as e:
        print(f"Error while adding a secret version: {e}")
        raise e
    
    return response


def add_and_destroy_prev(
    project_id: str, secret_id: str, data: SecretDataType, n_prev: int = 1, mute: bool = True
) -> None:
    """
    Adds a new secret version to the given secret with the provided data.
    Then, destory previous secret version(s).
    Args:
        project_id: the id of the GCP project where the secret is stored
        secret_id: the id of the secret to add a new version for
        data: data to store in the secret manager
        n_prev: how many previous secret versions to destroy (omitted if already destroy)
        mute: option to silence the success feedback message. (default True)
    Returns:
        None
    """
    add_version(project_id, secret_id, data, mute)
    secret_ver_info = get_version_info(project_id, secret_id, n_prev + 1) # n = 0 
    
    delete_list = []
    versions = secret_ver_info.get('version_id', [])
    state = secret_ver_info.get('state', [])
    
    for i, v_id in enumerate(versions):
        if i == 0 or state[i] != 1: # Do not delete the latest version or version that is already deleted
            continue
        delete_list.append(v_id)
    
    for v_id in delete_list:
        destroy_version(project_id, secret_id, v_id, mute)
    
    return None


def get_version(
    project_id: str, secret_id: str, version_id: str = "latest", mute: bool = True
) -> str | None:
    """
    Access the payload for the given secret version if one exists.
    arguments:
        project_id: the id of the GCP project where the secret is stored
        secret_id: the id of the secret to add a new version for
        version_id: the version number; default latest
            - the version_id can be a  number as a string (e.g. "5") or an alias (e.g. "latest")
        mute: option to silence the success feedback message. (default True)
    returns:
        secret:
            - the data stored in the Secret Manager for the given secret_id and version_id if it exists else None
    """
    secret = None
    
    try:
        client = secretmanager.SecretManagerServiceClient()
        rsc_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        # Access Secret Version
        response = client.access_secret_version(request={"name": rsc_name})
        secret = response.payload.data.decode("UTF-8")
        
        if not mute:
            print(f"Fetched the secret: '{response.name}'")
    
    except Exception as e:
        print(f"Error while getting the secret version: {e}")
        raise e
    
    return secret


def get_version_info(project_id: str, secret_id: str, top_n: int | None = None) -> dict[str, list]:
    """
    Fetches all or top N metadata for versions associated to the given secret.
    arguments:
        project_id: the id of the GCP project where the secret is stored
        secret_id: the id of the secret to add a new version for
        version_id: the version number; default latest
            - the version_id can be a  number as a string (e.g. "5") or an alias (e.g. "latest")
        top_n: the latest N versions to fetch info for
        mute: option to silence the success feedback message. (default True)
    returns:
        v_info: metadata for versions associated to the given secret
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the parent secret.
    parent = client.secret_path(project_id, secret_id)

    # List all secret versions.
    v_info: dict[str, list] = {}
    
    for i, v in enumerate(client.list_secret_versions(request={"parent": parent})):
        name_split = v.name.split("/")
        v_info.setdefault('secret_id', []).append(name_split[3])
        v_info.setdefault('version_id', []).append(name_split[5])
        v_info.setdefault('create_time', []).append(v.create_time)
        v_info.setdefault('state', []).append(v.state)
        
        if top_n and top_n == i + 1:
            break
    
    return v_info


def destroy_version(
    project_id: str, secret_id: str, version_id: str, mute: bool = True
) -> secretmanager.DestroySecretVersionRequest:
    """
    Destroy the given secret version, making the payload irrecoverable. Other
    secrets versions are unaffected.
    
    arguments:
        project_id: the id of the GCP project where the secret is stored
        secret_id: the id of the secret to destroy the version for
        version_id: the version number;
            - the version_id can be a  number as a string (e.g. "5") or an alias (e.g. "latest")
        mute: option to silence the success feedback message. (default True)
    returns:
        response: secretmanager.DestroySecretVersionRequest
    """
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Destroy the secret version.
        response = client.destroy_secret_version(request={"name": name})
        
        if not mute:
            print(f"Destroyed secret version: {response.name}")
    
    except Exception as e:
        print(f"Error while destroying the secret version: {e}")
        raise e
    
    return response
