from uuid import uuid3, NAMESPACE_DNS

DNS_FOR_NAMESPACE = 'sd2e.org'
UUID_NAMESPACE = uuid3(NAMESPACE_DNS, DNS_FOR_NAMESPACE)
STORAGE_ROOT = 'uploads/'

class Enumerations():
    LABS = ('ginkgo', 'transcriptic', 'biofab', 'emerald')
    CHALLENGE_PROBLEMS = ('yeast-gates', 'novel-chassis')
