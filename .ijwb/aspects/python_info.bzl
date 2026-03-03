# TEMPLATE-INCLUDE-BEGIN
# TEMPLATE-INCLUDE-END

def py_info_in_target(target):

# TEMPLATE-INCLUDE-BEGIN
  if PyInfo in target:
    return True
  return False
# TEMPLATE-INCLUDE-END

def get_py_info(target):

# TEMPLATE-INCLUDE-BEGIN
  if PyInfo in target:
    return target[PyInfo]
  return None
# TEMPLATE-INCLUDE-END

