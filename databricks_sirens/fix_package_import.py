'''
this is a workaround related to Databricks engineering making "databricks" a namespaced package. 
This code below makes it so we can use databricks.sirens modules without having to install a wheel file.

To use this, simply add this to the top of your notebook.

import databricks_sirens.fix_package_import
'''
try:
  import databricks
  from os.path import dirname, realpath, sep, pardir
  databricks.__path__.append(dirname(dirname(realpath(__file__))) + sep + 'databricks')
except Exception as e:
  print('Failed to fix package import', e)

try:
  import databricks.sirens
except:
  print('Failed to import databricks.sirens', e)