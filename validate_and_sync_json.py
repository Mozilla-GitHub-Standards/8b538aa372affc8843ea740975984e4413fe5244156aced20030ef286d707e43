import sys
import yaml
import json
import boto.s3.connection
import boto.s3.key


def validate_json(experiments_string, experiments_filename):
    # check for valid json
    try:
        json.loads(experiments_string)
    except ValueError:
        sys.exit("Invalid JSON in experiments file: {0}".format(
            experiments_filename))


def get_experiments_string(experiments_filename):
    try:
        experiments_file = open(experiments_filename, 'rb')
    except IOError:
        sys.exit("Could not read experiments file: {0}".format(
            experiments_filename))

    # save experiments text in string
    experiments = experiments_file.read()
    experiments_file.close()

    return experiments


def get_config(config_filename):
    # open config file
    try:
        config_file = open(config_filename, 'rb')
    except IOError:
        sys.exit("Could not open configuration file: {0}".format(
            config_filename))

    # parse config
    try:
        y = yaml.load(config_file.read())
    except yaml.YAMLError:
        sys.exit("Error in configuration file: {0}".format(
            config_filename))

    # done with config file after reading
    config_file.close()

    # check for value existence
    if "experiments_filename" not in y:
        sys.exit("No value for 'experiments_filename' in config {0}".format(
            config_filename))
    if "bucket" not in y:
        sys.exit("No value for 'bucket' in config {0}".format(
            config_filename))
    if "upload" not in y:
        sys.exit("No value for 'upload' in config {0}".format(
            config_filename))

    return y


def main():
    if len(sys.argv) != 2:
        sys.exit("{0} accepts one argument, path to a YAML configuration file".format(
            sys.argv[0]))
    config_filename = sys.argv[1]

    config = get_config(config_filename)

    bucket = config.get("bucket")
    upload = config.get("upload")
    experiments_filename = config.get("experiments_filename")

    experiments_string = get_experiments_string(
        experiments_filename)
    print "*** {0} file contents ***\n{1}".format(experiments_filename, experiments_string)

    validate_json(
        experiments_string, experiments_filename)

    if not upload:
        sys.exit("Upload is disabled in config")

    # initialize a boto
    conn = boto.s3.connection.S3Connection()
    bucket = conn.get_bucket(bucket)

    # new key for experiments file
    k = boto.s3.key.Key(bucket)
    k.key = experiments_filename
    k.set_contents_from_string(experiments_string)

    # success
    print "Uploaded {0} to s3".format(experiments_filename)

if __name__ == "__main__":
    main()
