import logging
import os
import re
import zipfile

logging.getLogger().setLevel(logging.INFO)


class BaseCsv:
    """
    Handles functions that help working with multiple coherent csv files
    """

    @staticmethod
    def list_all_files_inside_dir(path):
        onlyfiles = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        sortet_files = sorted(onlyfiles)
        return sortet_files

    @staticmethod
    def list_all_regex_files_inside_dir(path, regex):
        return [f for f in os.listdir(path) if re.match(r"{}".format(regex), f)]

    @staticmethod
    def sort_list(list):
        """Sort list on splitting with first '-' and '.'"""
        return sorted(list, key=lambda x: int(x.split("-")[1].split(".")[0]))

    @staticmethod
    def sort_list_on_splitter(list, split1, split2):
        """Sort list on splitting with first '-' and '.'"""
        return sorted(list, key=lambda x: int(x.split(split1)[1].split(split2)[0]))

    @staticmethod
    def split(
        filehandler,
        delimiter=",",
        row_limit=10000,
        output_name_template="%s.csv",
        output_path=".",
        keep_headers=True,
    ):
        """
        split(open('/your/path/input.csv', 'r'));

        Args:
            filehandler (_type_): _description_
            delimiter (str, optional): _description_. Defaults to ','.
            row_limit (int, optional): _description_. Defaults to 10000.
            output_name_template (str, optional): _description_. Defaults to 'output_%s.csv'.
            output_path (str, optional): _description_. Defaults to '.'.
            keep_headers (bool, optional): _description_. Defaults to True.
        """
        import csv

        reader = csv.reader(filehandler, delimiter=delimiter)
        current_piece = 1
        current_out_path = os.path.join(output_path, output_name_template % current_piece)
        current_out_writer = csv.writer(open(current_out_path, "w"), delimiter=delimiter)
        current_limit = row_limit
        if keep_headers:
            headers = next(reader)
            current_out_writer.writerow(headers)
        for i, row in enumerate(reader):
            if i + 1 > current_limit:
                current_piece += 1
                current_limit = row_limit * current_piece
                current_out_path = os.path.join(output_path, output_name_template % current_piece)
                current_out_writer = csv.writer(open(current_out_path, "w"), delimiter=delimiter)
                if keep_headers:
                    current_out_writer.writerow(headers)
            current_out_writer.writerow(row)

    @staticmethod
    def extract_zip(zip_file, targer_dir):
        """
        Attention: path should not contain "work"
        """
        try:
            with zipfile.ZipFile(
                zip_file, "r", compression="ZIP_STORED", allowZip64=True
            ) as zip_ref:
                zip_ref.extractall(targer_dir)
        except OSError as err:
            print(err)
            logging.warning("ZipFile start \n {} \n Zipfile end".format(err))
