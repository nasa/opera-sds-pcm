import re
import os
import fnmatch
import hashlib
from hysds.utils import calculate_checksum_from_localized_file


def create_dataset_checksums(dataset_dir, algo, globs=[], regex=[]):
    """
     Create checksum files for files in a directory using calculated using the specified algorithm.

     This function creates the checksum files for the files in the directory using the specified algorithm.
     The files that are subjected to checksum are filtered using the specified globs or regular expressions.

     @param dataset_dir (string) - The directory containing the files
     @param algo (string) - The algorithm used to calculate the checksum
     @param globs (list) - A list of glob for filtering files
     @param regex (list) - A list of regular expression for filtering files

     @return Checksum files with the original file name with the checksum algorithm name as extension

     """
    is_file = os.path.isfile(dataset_dir)
    glob_only = False
    regex_only = False
    both = False
    no_filter = False

    if not globs and not regex:
        no_filter = True
    if not globs and regex:
        regex_only = True
    if globs and not regex:
        glob_only = True
    if globs and regex:
        both = True

    if is_file:
        cfile = os.path.join(dataset_dir + "." + algo)
        f = open(cfile, "w+")
        f.write(calculate_checksum_from_localized_file(dataset_dir, algo))
        f.close()
    else:
        if no_filter:
            for dirName, subdirList, fileList in os.walk(dataset_dir):
                for fname in fileList:
                    cfile = os.path.join(dirName, fname + "." + algo)
                    f = open(cfile, "w+")
                    f.write(
                        calculate_checksum_from_localized_file(
                            os.path.join(dirName, fname), algo
                        )
                    )
                    f.close()
        if glob_only:
            for dirName, subdirList, fileList in os.walk(dataset_dir):
                for fname in fileList:
                    for g in globs:
                        if fnmatch.fnmatch(fname, g):
                            cfile = os.path.join(dirName, fname + "." + algo)
                            f = open(cfile, "w+")
                            f.write(
                                calculate_checksum_from_localized_file(
                                    os.path.join(dirName, fname), algo
                                )
                            )
                            f.close()
        if regex_only:
            for dirName, subdirList, fileList in os.walk(dataset_dir):
                for fname in fileList:
                    for r in regex:
                        if re.match(r, fname):
                            cfile = os.path.join(dirName, fname + "." + algo)
                            f = open(cfile, "w+")
                            f.write(
                                calculate_checksum_from_localized_file(
                                    os.path.join(dirName, fname), algo
                                )
                            )
                            f.close()
        if both:
            for dirName, subdirList, fileList in os.walk(dataset_dir):
                for fname in fileList:
                    for g in globs:
                        if fnmatch.fnmatch(fname, g):
                            cfile = os.path.join(dirName, fname + "." + algo)
                            f = open(cfile, "w+")
                            f.write(
                                calculate_checksum_from_localized_file(
                                    os.path.join(dirName, fname), algo
                                )
                            )
                            f.close()
                    for r in regex:
                        if re.match(r, fname):
                            cfile = os.path.join(dirName, fname + "." + algo)
                            f = open(cfile, "w+")
                            f.write(
                                calculate_checksum_from_localized_file(
                                    os.path.join(dirName, fname), algo
                                )
                            )
                            f.close()


def get_file_checksum(file_content, checksum_type):
    """
        Perform checksum depending on which type.
        :param file_content:
        :param checksum_type:
        :return:
        """
    if checksum_type == "md5":
        return hashlib.md5(file_content).hexdigest()
    elif checksum_type == "sha1":
        return hashlib.sha1(file_content).hexdigest()
    elif checksum_type == "sha224":
        return hashlib.sha224(file_content).hexdigest()
    elif checksum_type == "sha256":
        return hashlib.sha256(file_content).hexdigest()
    elif checksum_type == "sha384":
        return hashlib.sha384(file_content).hexdigest()
    elif checksum_type == "sha512":
        return hashlib.sha512(file_content).hexdigest()
    else:
        raise RuntimeError("Invalid checksum type : {}".format(checksum_type))
