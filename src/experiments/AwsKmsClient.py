import pyarrow.parquet.encryption as pe
import base64

class AwsKmsClient(pe.KmsClient):
    """
    Minimal AWS KMS client for Parquet Modular Encryption.
    Parquet uses this object to wrap/unwrap DEKs.
    """
    def __init__(self, kms_client, key_map):
        super().__init__()
        self.kms = kms_client
        self.key_map = key_map

    def wrap_key(self, key_bytes, master_key_id):
        cmk_arn = self.key_map[master_key_id]
        resp = self.kms.encrypt(
            KeyId=cmk_arn,
            Plaintext=key_bytes
        )
        cipher_blob = resp["CiphertextBlob"]
        return base64.b64encode(cipher_blob)

    def unwrap_key(self, wrapped_key, master_key_id):
        cmk_arn = self.key_map[master_key_id]
        cipher_blob = base64.b64decode(wrapped_key)

        resp = self.kms.decrypt(
            KeyId=cmk_arn,
            CiphertextBlob=cipher_blob,
        )

        return resp["Plaintext"]