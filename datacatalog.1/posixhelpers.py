import os
import hashlib
import binascii
import filetype

def get_size_in_bytes(posix_path):
    """Safely returns file size in bytes"""
    try:
        if os.path.isfile(posix_path):
            return os.path.getsize(posix_path)
        else:
            return 0
    except Exception:
        return -1

def compute_checksum(posix_path, fake_checksum=False):
    """Approved method to generate file checksums"""
    if fake_checksum:
        return binascii.hexlify(os.urandom(20)).decode()

    hash_sha = hashlib.sha1()
    with open(posix_path, "rb") as f:
        for chunk in iter(lambda: f.read(131072), b""):
            hash_sha.update(chunk)
    return hash_sha.hexdigest()


def validate_checksum(posix_path, known_checksum):
    """Validate checksum of a file and return Boolean response"""
    computed = compute_checksum(posix_path, False)
    if computed == known_checksum:
        return True
    else:
        return False

def guess_mimetype(posix_path, default='text/plaintext'):
    ftype = filetype.guess(posix_path)
    if ftype is None:
        return default
    else:
        return ftype.mime

def get_filetype(posix_path):
    # Returns the file type. This is a stub for more sophisticated mechanism
    return guess_mimetype(posix_path)


def splitall(path):
    # Returns a list of all path elements
    # Ref: https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch04s16.html
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts
