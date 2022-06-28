from os import getenv
import os.path as P
from io import BytesIO


class CAOS_client:
    """Cloud-Agnostic Object Storage client"""

    def __init__(self, provider: str, *args, **kwargs):

        self.__ls = None
        self.__exists = None
        self.__sparkify = None
        self.__open = None
        self.__provider_inits = {
            "aws": self.__aws_s3_client,
            # "azure": self.__azure_blob_storage_client,
            # "gcp": self.__gcp_storage_client,
            # alibaba if china takes over the world dreadfully
        }

        supported_providers = self.__provider_inits.keys()
        if provider not in supported_providers:
            error_msg = f'provider {provider} not supported. Try {", ".join(supported_providers)}'
            raise NotImplementedError(error_msg)

        self.provider = provider
        self.__provider_inits[provider](self, *args, **kwargs)

    def __aws_s3_client(self, *args, **kwargs):
        import s3fs

        creds = {k: v for k, v in kwargs.items() if k in ("key", "secret", "token")}
        s3 = s3fs.S3FileSystem(**creds)

        self.__ls = s3.ls
        self.__exists = s3.exists
        self.__open = s3.open
        self.__sparkify = lambda path: P.join("s3a://", path)

    # TODO: def __azure_blob_storage_client(self, *args, **kwargs):

    @property
    def ls(self):
        return self.__ls

    @property
    def exists(self):
        return self.__exists

    @property
    def open(self):
        return self.__open

    @property
    def sparkify(self):
        return self.__sparkify
