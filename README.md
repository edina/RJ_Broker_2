#h RJ_Broker_2

This is the SWORD 2 importer to receive deposit packages from the Repository Junction Broker. 

This is the EPrints Bazaar package that is compatible with EPrints 3.3
For EPrints 3.2, use https://github.com/edina/RJ_Broker_Importer_3.2

It has been tested with 3.3.5 (the first released version) and 3.3.12 (the current version at the time of release)

##h INSTALLATION

###h from GitHub

1) Download & unpack the .zip file, or clone the .git repository

2) Use the "Choose File" at the bottom of the "Available packages" list to upload & install the RJ_Broker_2.epm file

###h from the EPrints Bazaar

1) Install the package from the Bazaar

##h CONFIGURATION

You can check that the new configuration has been loaded by getting the SWORD servicedocument & looking for the sword:acceptPackaging with a value of "http://opendepot.org/broker/1.0"


##h TESTING

The directory "example export files" contains some sample records, along with instructions for testing the raw deposit process.
