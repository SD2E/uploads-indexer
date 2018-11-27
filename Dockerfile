FROM sd2e/reactors:python3-edge

RUN pip uninstall --yes datacatalog
# # COPY datacatalog /datacatalog

RUN pip3 install --upgrade git+https://github.com/SD2E/python-datacatalog.git@composed_schema
