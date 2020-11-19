# sphinx-documentation-experimental
Experimental repo for trying out auto doc [it is actually semi-auto] using sphinx. 

## Steps to kick-start:

1. Install sphinx: pip install -U Sphinx or  brew install sphinx-doc.
    Tryout the tutorial on the page just to playaround a bit: https://matplotlib.org/sampledoc/getting_started.html#installing-your-doc-directory
2. For a sphinx project setup, use this command outside or inside of the repo you are trying to get the auto doc for: sphinx_quickstart which sets up the basic folders. Follow through the command prompt this up initially.
    Refer to this link for experimenting with auto-doc config: https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#module-sphinx.ext.autodoc
3. Run this command to create the rst file for each module: sphinx-apidoc -fo <destination_folder_for_all_rst_files> <path_to_module>. 
4. After making changes to say, index.rst -> run `make clean` and then `make build`.
5. Open up the local static page from here -> `build/html/index.html`.

