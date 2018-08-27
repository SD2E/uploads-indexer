from uuid import uuid3, NAMESPACE_DNS

DNS_FOR_NAMESPACE = 'sd2e.org'
UUID_NAMESPACE = uuid3(NAMESPACE_DNS, DNS_FOR_NAMESPACE)
STORAGE_ROOT = 'uploads/'

class Constants():
    DNS_FOR_NAMESPACE = 'sd2e.org'
    UUID_NAMESPACE = uuid3(NAMESPACE_DNS, DNS_FOR_NAMESPACE)
    STORAGE_ROOT = 'uploads/'
class Enumerations():
    LABPATHS = ('ginkgo', 'transcriptic', 'biofab', 'emerald')
    LABNAMES = ('Ginkgo', 'Transcriptic', 'UW_BIOFAB', 'Emerald')
    CHALLENGE_PROBLEMS = ('yeast-gates', 'novel-chassis')

class Mappings():
    LABPATHS = {'ginkgo': 'Ginkgo', 'transcriptic': 'Transcriptic', 'biofab': 'UW_BIOFAB', 'emerald': 'Emerald'}
