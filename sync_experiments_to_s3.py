from __future__ import with_statement
import sys, yaml, json, boto.s3.connection, boto.s3.key

def main():
  if len(sys.argv) != 2:
    sys.exit("{0} accepts one argument, path to a YAML configuration file".format(sys.argv[0]))
  config_filename = sys.argv[1]

  # open config
  try:
    config_file = open(config_filename, 'rb')
  except IOError:
    sys.exit("Could not open configuration file: {0}".format(config_filename))

  with config_file:
    # parse config
    try:
      y = yaml.load(config_file.read())
    except yaml.YAMLError:
      sys.exit("Error in configuration file: {0}".format(config_filename))

    # check for value existence
    if not y.get("experiments_filename"):
      sys.exit("No value for 'experiments_filename' in config {0}".format(config_filename))
    if not y.get("bucket"):
      sys.exit("No value for 'bucket' in config {0}".format(config_filename))
    if not y.get("upload"):
      sys.exit("No value for 'upload' in config {0}".format(config_filename))

    # done with config file after parsing
    config_file.close()

    bucket = y.get("bucket")
    upload = y.get("upload")
    experiments_filename = y.get("experiments_filename")

    if not upload:
      sys.exit("Upload is disabled in config")

    try:
      experiments_file = open(experiments_filename, 'rb')
    except IOError:
      sys.exit("Could not read experiments file: {0}".format(experiments_filename))

    with experiments_file:
      # save experiments text in string
      experiments = experiments_file.read()
      experiments_file.close()

      print "*** {0} file contents ***\n{1}".format(experiments_filename, experiments)

      # check for valid json
      try:
        j = json.loads(experiments)
      except ValueError:
        sys.exit("Invalid JSON in experiments file: {0}".format(experiments_filename))

      # initialize a boto
      conn = boto.s3.connection.S3Connection()
      bucket = conn.get_bucket(bucket)

      # new key for experiments file
      k = boto.s3.key.Key(bucket)
      k.key = experiments_filename
      k.set_contents_from_string(experiments)

      # success
      print "Uploaded {0} to s3".format(experiments_filename)

if __name__ == "__main__":
  main()
