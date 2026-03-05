provider_attrs = ["xcode_version", "default_macos_sdk_version"]

def all_items_are_true(items):
    for item in items:
        if item == False:
            return False

    return True

def hasattrs(obj, attrs):
    return all_items_are_true([hasattr(obj, attr) for attr in attrs])

def format(target):
    all_providers = providers(target)
    for key in all_providers:
        provider = all_providers[key]

        if hasattrs(provider, provider_attrs):
            attrs = [getattr(provider, attr) for attr in provider_attrs]
            return "{} {}".format(attrs[0], attrs[1])

    return ""
