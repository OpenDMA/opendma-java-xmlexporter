# OpenDMA XML Exporter

Exports subset of objects in an OpenDMA repository to an XML file which can be consumed by
[opendma-java-xmlrepo](https://github.com/OpenDMA/opendma-java-xmlrepo)

## Usage

Make sure to have the opendma-api and the OpenDMA Adaptor for your repository on the classpath.

```
Usage: XMLExporter <properties-file>

The properties file contains a key-value list defining the source and target of this export.
Possible keys are:
SystemProperty.xxxx : Will be set as Java system property xxxx befor Export
AdaptorSystemId     : The system ID of the Adaptor to be used
Session.xxxx        : Set property xxxx for session setup
                      Most adaptors require at least these session properties:
                      Session.user=username
                      Session.password=password
Repository          : The ID of the repository to be exported
ExcludeClasses      : blank separated list of classnames to be excluded from export
ExcludeIds          : blank separated list of IDs of objects to be excluded from export
Outfile             : The file where the XML export is written to. Default is OpenDMA.xml
ContentDirectory    : The directory where data files are written to. Only if ExportContent=true. Default is 'data'
ExportContent       : true/false Export also Content. Default is false
Verbose             : 0/1/2 Degree of progress messages. Default is 1
```