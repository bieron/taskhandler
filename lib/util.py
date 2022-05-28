import yaml

def loadSchedule(path):
  return yaml.load(path, Loader=yaml.SafeLoader)
