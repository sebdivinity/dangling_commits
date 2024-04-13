from enum import Enum, auto


class CommitStatus(Enum):
    UNKNOWN = auto()
    ERASED = auto()
    INCOMPLETE = auto()
    FOUND = auto()


class CommitSignatureStatus(Enum):
    UNSIGNED = auto()  # Unsigned.
    VALID = auto()  # Valid signature and verified by GitHub.
    NO_USER = auto()  # Email used for signing not known to GitHub.
    UNKNOWN_KEY = auto()  # Key used for signing not known to GitHub.
    BAD_CERT = auto()  # The signing certificate or its chain could not be verified.
    BAD_EMAIL = auto()  # Invalid email used for signing.
    EXPIRED_KEY = auto()  # Signing key expired.
    GPGVERIFY_ERROR = auto()  # Internal error - the GPG verification service misbehaved.
    # Internal error - the GPG verification service is unavailable at the moment.
    GPGVERIFY_UNAVAILABLE = auto()
    INVALID = auto()  # Invalid signature.
    MALFORMED_SIG = auto()  # Malformed signature.
    NOT_SIGNING_KEY = auto()  # The usage flags for the key that signed this don't allow signing.
    OCSP_ERROR = auto()  # Valid signature, though certificate revocation check failed.
    OCSP_PENDING = auto()  # Valid signature, pending certificate revocation checking.
    OCSP_REVOKED = auto()  # One or more certificates in chain has been revoked.
    UNKNOWN_SIG_TYPE = auto()  # Unknown signature type.
    UNVERIFIED_EMAIL = auto()  # Email used for signing unverified on GitHub.
