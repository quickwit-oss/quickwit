# TEMPLATE-INCLUDE-BEGIN
# TEMPLATE-INCLUDE-END

def java_info_in_target(target):

# TEMPLATE-INCLUDE-BEGIN
  return JavaInfo in target
# TEMPLATE-INCLUDE-END

def get_java_info(target):

# TEMPLATE-INCLUDE-BEGIN
  if JavaInfo in target:
      return target[JavaInfo]
  else:
      return None
# TEMPLATE-INCLUDE-END

def java_info_reference():

# TEMPLATE-INCLUDE-BEGIN
  return [JavaInfo]
# TEMPLATE-INCLUDE-END


def get_provider_from_target(provider_name, target):

# TEMPLATE-INCLUDE-BEGIN
  provider = getattr(java_common, provider_name, None)
  return target[provider] if provider and provider in target else None
# TEMPLATE-INCLUDE-END