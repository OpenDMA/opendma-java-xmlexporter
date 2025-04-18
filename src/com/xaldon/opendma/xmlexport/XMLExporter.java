package com.xaldon.opendma.xmlexport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.opendma.AdaptorManager;
import org.opendma.api.OdmaClass;
import org.opendma.api.OdmaContent;
import org.opendma.api.OdmaId;
import org.opendma.api.OdmaObject;
import org.opendma.api.OdmaProperty;
import org.opendma.api.OdmaPropertyInfo;
import org.opendma.api.OdmaQName;
import org.opendma.api.OdmaRepository;
import org.opendma.api.OdmaSession;
import org.opendma.api.OdmaType;
import org.opendma.exceptions.OdmaObjectNotFoundException;

public class XMLExporter
{

    /**
     * The main method so this class can be used as stand-alone console application.
     * 
     * @param args The command line arguments
     */
    public static void main(String[] args)
    {
        System.out.println("OpenDMA XML Exporter version 0.7.0");
        System.out.println("Copyright (c) 2011-2025 xaldon Technologies GmbH");
        if(args.length != 1)
        {
            System.out.println();
            System.out.println("Usage: XMLExporter <properties-file>");
            System.out.println();
            System.out.println("The properties file contains a key-value list defining the source and target of this export.");
            System.out.println("Possible keys are:");
            System.out.println("SystemProperty.xxxx : Will be set as Java system property xxxx befor Export");
            System.out.println("AdaptorClass        : The exporter will try to register an OpenDMA Adaptor from this class");
            System.out.println("AdaptorSystemId     : The system ID of the Adaptor to be used");
            System.out.println("Session.xxxx        : Set property xxxx for session setup");
            System.out.println("                      Most adaptors require at least these session properties:");
            System.out.println("                      Session.user=username");
            System.out.println("                      Session.password=password");
            System.out.println("Repository          : The ID of the repository to be exported");
            System.out.println("ExcludeClasses      : blank separated list of classnames to be excluded from export");
            System.out.println("ExcludeIds          : blank separated list of IDs of objects to be excluded from export");
            System.out.println("Outfile             : The file where the XML export is written to. Default is OpenDMA.xml");
            System.out.println("ContentDirectory    : The directory where data files are written to. Only if ExportContent=true. Default is 'data'");
            System.out.println("ExportContent       : true/false Export also Content. Default is false");
            System.out.println("Verbose             : 0/1/2 Degree of progress messages. Default is 1");
            System.exit(1);
        }
        Properties exportProperties = new Properties();
        try
        {
            exportProperties.load(new FileInputStream(args[0]));
        }
        catch(FileNotFoundException e)
        {
            System.out.println("Properties file '"+args[0]+"' can not be found.");
            System.exit(1);
        }
        catch (IOException e)
        {
            System.out.println("Error reading properties file '"+args[0]+"':");
            e.printStackTrace(System.out);
            System.exit(1);
        }
        try
        {
            XMLExporter xmlExporter = new XMLExporter(exportProperties);
            xmlExporter.runExport();
        }
        catch(Exception e)
        {
            System.out.println("Error performing Export:");
            e.printStackTrace(System.out);
        }
    }
    
    protected Properties sessionProperties = new Properties();
    
    protected String repositoryId = null;
    
    protected List<Pattern> excludeClasses = new ArrayList<Pattern>();
    
    protected List<String> excludeIds = new ArrayList<String>();
    
    protected String outfile = null;
    
    protected String contentDirectory = null;
    
    protected boolean exportContent = false;
    
    protected int verbose = 1;
    
    protected int exportetContentIdCounter = 1;
    
    protected HashMap<String,Object> exportedObjects = new HashMap<String,Object>();
    
    protected LinkedHashMap<String,OdmaQName> exportQueue = new LinkedHashMap<String,OdmaQName>();
    
    /**
     * Create a new XMLExporter configured from the given properties.
     * 
     * @param props the Properties to get the configuration from
     * 
     * @throws NullPointerException if props is <code>null</code>
     * @throws IllegalArgumentException if the configuration is not valid
     */
    public XMLExporter(Properties props) throws Exception
    {
        // assemble session properties and set system properties
        Iterator<Entry<Object,Object>> itPropEntries = props.entrySet().iterator();
        while(itPropEntries.hasNext())
        {
            Entry<Object,Object> prop = itPropEntries.next();
            String propName = prop.getKey().toString();
            String propValue = prop.getValue().toString();
            if(propName.startsWith("SystemProperty."))
            {
                System.setProperty(propName.substring(15), propValue);
            }
            if(propName.startsWith("Session."))
            {
                sessionProperties.setProperty(propName.substring(8), propValue);
            }
        }
        // configure from properties
        repositoryId = props.getProperty("Repository","");
        String excludeClassesConfig = props.getProperty("ExcludeClasses");
        if(excludeClassesConfig != null)
        {
            String[] excludeClassesList = excludeClassesConfig.split(" ");
            for(String patternString : excludeClassesList)
            {
                excludeClasses.add(Pattern.compile(patternString));
            }
        }
        String excludeIdsConfig = props.getProperty("ExcludeIds");
        if(excludeIdsConfig != null)
        {
            String[] excludeIdsList = excludeIdsConfig.split(" ");
            for(String excludedId : excludeIdsList)
            {
                excludeIds.add(excludedId);
            }
        }
        outfile = props.getProperty("Outfile","OpenDMA.xml");
        contentDirectory = props.getProperty("ContentDirectory","data");
        String exportContentConfig = props.getProperty("ExportContent");
        if(exportContentConfig != null)
        {
            if(exportContentConfig.equalsIgnoreCase("true"))
            {
                exportContent = true;
            }
            else if(exportContentConfig.equalsIgnoreCase("false"))
            {
                exportContent = false;
            }
            else
            {
                throw new IllegalArgumentException("Invalid value for ExportContent configuration property. Possible values are 'true' or 'false'");
            }
        }
        String verboseConfig = props.getProperty("Verbose");
        if(verboseConfig != null)
        {
            try
            {
                verbose = Integer.parseInt(verboseConfig);
            }
            catch(NumberFormatException nfe)
            {
                throw new IllegalArgumentException("Invalid value for Verbose. Possible values are 0,1,2");
            }
        }
        // try to load the OpenDMA adaptor
        String adaptorClassName = props.getProperty("AdaptorClass");
        if(adaptorClassName != null)
        {
            Class.forName(adaptorClassName);
        }
    }
    
    public void runExport() throws Exception
    {
        // establish session
        OdmaSession session = AdaptorManager.getSession(sessionProperties);
        // get the repository to be exported
        OdmaRepository repo = session.getRepository(new OdmaId(repositoryId));
        // create output file
        PrintStream outStream = new PrintStream(new FileOutputStream(outfile),false,"UTF-8");
        // perform the export
        if(verbose > 0)
        {
            System.out.println("Performing export...");
        }
        doExport(outStream, session, repo);
        if(verbose > 0)
        {
            System.out.println("Export finished.");
        }
        // flush and close output stream
        outStream.flush();
        outStream.close();
    }
    
    public void doExport(PrintStream out, OdmaSession session, OdmaRepository repo) throws Exception
    {
        // print header of XML file
        out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        out.println("<OdmaXmlRepository xmlns=\"http://www.opendma.org/XMLRepository\" repositoryObjectId=\""+repo.getId().toString()+"\">");
        // export the repository object itself
        if(verbose > 0)
        {
            System.out.println("Exporting Repository object...");
        }
        dumpObject(out,repo);
        // export the class tree
        dumpClassTreeIter(out,repo.getRootClass());
        repo = null;
        if(verbose > 0)
        {
            System.out.println("Exporting referenced objects...");
        }
        while(exportQueue.size() > 0)
        {
            // get first entry
            Map.Entry<String, OdmaQName> e = exportQueue.entrySet().iterator().next();
            exportQueue.remove(e.getKey());
            if(verbose > 0)
            {
                System.out.println("Exporting referenced object "+e.getKey()+" ("+e.getValue()+")");
            }
            try
            {
                OdmaObject refObj = session.getObject(new OdmaId(repositoryId), new OdmaId(e.getKey()), null);
                dumpObject(out,refObj);
            }
            catch(OdmaObjectNotFoundException onfe)
            {
                System.out.println("  Error: object not found.");
            }
            catch(Exception ex)
            {
                System.out.println("  Error getting object:");
                ex.printStackTrace(System.out);
            }
        }
        // print footer of XML file
        out.println("</OdmaXmlRepository>");
    }
    
    public void dumpObject(PrintStream out, OdmaObject obj) throws Exception
    {
        exportQueue.remove(obj.getId().toString());
        if(exportedObjects.containsKey(obj.getId().toString()))
        {
            System.out.println("WARNING: tried to export an already exported object: "+obj.getId().toString());
            return;
        }
        if(verbose >= 2)
        {
            System.out.println("    > "+obj.getId());
        }
        exportedObjects.put(obj.getId().toString(), null);
        LinkedHashMap<String,OdmaObject> nonRetrievableObjects = new LinkedHashMap<String,OdmaObject>();
        out.println("    <OdmaObject classNamespace=\""+obj.getOdmaClass().getNamespace()+"\" className=\""+obj.getOdmaClass().getName()+"\">");
        Iterable<OdmaPropertyInfo> props = obj.getOdmaClass().getProperties();
        if(props != null)
        {
            Iterator<?> it = props.iterator();
            while(it.hasNext())
            {
                OdmaPropertyInfo pi = (OdmaPropertyInfo)it.next();
                if(verbose >= 2)
                {
                    System.out.println("        > "+pi.getQName());
                }
                try
                {
                    dumpProperty(out, pi, obj, nonRetrievableObjects);
                }
                catch(Exception e)
                {
                    System.out.println("----> Error dumping property "+pi.getQName()+" of object "+obj.getId());
                    e.printStackTrace(System.out);
                }
            }
        }
        out.println("    </OdmaObject>");
        obj = null;
        while(nonRetrievableObjects.size() > 0)
        {
            Entry<String,OdmaObject> nroEntry = nonRetrievableObjects.entrySet().iterator().next();
            OdmaObject volObj = nroEntry.getValue();
            nonRetrievableObjects.remove(nroEntry.getKey());
            if(exportQueue.containsKey(volObj.getId().toString()))
            {
                System.out.println("WARNING: ID of non-retrieval object has been found in the export queue. This is an indicator for duplicate IDs in the repository.");
            }
            if(exportedObjects.containsKey(volObj.getId().toString()))
            {
                System.out.println("WARNING: tried to export an already exported object: "+volObj.getId().toString());
                return;
            }
            if(verbose >= 2)
            {
                System.out.println("    >> "+volObj.getId()+" ("+volObj.getOdmaClass().getQName()+")");
            }
            exportedObjects.put(volObj.getId().toString(), null);
            out.println("    <OdmaObject classNamespace=\""+volObj.getOdmaClass().getNamespace()+"\" className=\""+volObj.getOdmaClass().getName()+"\">");
            props = volObj.getOdmaClass().getProperties();
            if(props != null)
            {
                Iterator<?> it = props.iterator();
                while(it.hasNext())
                {
                    OdmaPropertyInfo pi = (OdmaPropertyInfo)it.next();
                    if(verbose >= 2)
                    {
                        System.out.println("        >> "+pi.getQName());
                    }
                    try
                    {
                        dumpProperty(out, pi, volObj, nonRetrievableObjects);
                    }
                    catch(Exception e)
                    {
                        System.out.println("----> Error dumping property "+pi.getQName()+" of non-retrievable object "+volObj.getId());
                        e.printStackTrace(System.out);
                    }
                }
            }
            out.println("    </OdmaObject>");
        }
    }
    
    public void dumpProperty(PrintStream out, OdmaPropertyInfo pi, OdmaObject obj, LinkedHashMap<String,OdmaObject> nonRetrievableObjects) throws Exception
    {
        if(pi.getDataType() == OdmaType.GUID.getNumericId())
        {
            return;
        }
        String typeName = datatypeValues.get(new Integer(pi.getDataType()));
        if(typeName == null)
        {
            throw new RuntimeException("no data type for "+pi.getDataType());
        }
        out.print("        <Property namespace=\""+pi.getNamespace()+"\" name=\""+pi.getName()+"\" type=\""+typeName+"\" multiValue=\""+(pi.isMultiValue()?"true":"false")+"\">");
        try
        {
            OdmaProperty prop = obj.getProperty(pi.getQName());
            dumpPropertyValues(out,prop,pi,nonRetrievableObjects);
        }
        catch(Exception e)
        {
            System.out.println("----> Error dumping value of property "+pi.getQName()+" of object "+obj.getId()+" ("+obj.getOdmaClass().getQName()+")");
            e.printStackTrace(System.out);
        }
        out.println("</Property>");
    }
    
    public void dumpPropertyValues(PrintStream out, OdmaProperty prop, OdmaPropertyInfo pi, LinkedHashMap<String,OdmaObject> nonRetrievableObjects) throws Exception
    {
        if(prop.isMultiValue())
        {
            dumpPropertyMultivalue(out,prop,nonRetrievableObjects);
        }
        else
        {
            dumpPropertySinglevalue(out,prop,nonRetrievableObjects);
        }
    }
    
    public void dumpPropertyMultivalue(PrintStream out, OdmaProperty prop, LinkedHashMap<String,OdmaObject> nonRetrievableObjects) throws Exception
    {
        if(prop.getType() == OdmaType.REFERENCE)
        {
            Iterable<? extends OdmaObject> objEnum = prop.getReferenceIterable();
            if(objEnum != null)
            {
                Iterator<? extends OdmaObject> itObjectEnum = objEnum.iterator();
                while(itObjectEnum.hasNext())
                {
                    OdmaObject odmaObj = itObjectEnum.next();
                    dumpPropertyValueObject(out,odmaObj,OdmaType.REFERENCE,prop.getName(),nonRetrievableObjects);
                }
            }
        }
        else
        {
            List<?> lst = (List<?>)prop.getValue();
            if(lst != null)
            {
                for(int i = 0; i < lst.size(); i++)
                {
                    dumpPropertyValueObject(out,lst.get(i),prop.getType(),prop.getName(),nonRetrievableObjects);
                }
            }
        }
    }
    
    public void dumpPropertySinglevalue(PrintStream out, OdmaProperty prop, LinkedHashMap<String,OdmaObject> nonRetrievableObjects) throws Exception
    {
        Object valueObj = prop.getValue();
        if(valueObj == null)
        {
            return;
        }
        OdmaType type = prop.getType();
        dumpPropertyValueObject(out,valueObj,type,prop.getName(),nonRetrievableObjects);
    }
    
    public void dumpPropertyValueObject(PrintStream out, Object value, OdmaType type, OdmaQName propQName, LinkedHashMap<String,OdmaObject> nonRetrievableObjects)
    {
        switch(type)
        {
        case STRING:
            out.print("<Value>");
            dumpXMLString(out,(String)value);
            out.print("</Value>");
            break;
        case INTEGER:
            out.print("<Value>");
            out.print(value.toString());
            out.print("</Value>");
            break;
        case SHORT:
            out.print("<Value>");
            out.print(value.toString());
            out.print("</Value>");
            break;
        case LONG:
            out.print("<Value>");
            out.print(value.toString());
            out.print("</Value>");
            break;
        case FLOAT:
            out.print("<Value>");
            out.print(value.toString());
            out.print("</Value>");
            break;
        case DOUBLE:
            out.print("<Value>");
            out.print(value.toString());
            out.print("</Value>");
            break;
        case BOOLEAN:
            out.print("<Value>");
            out.print(((Boolean)value).booleanValue() ? "true" : "false");
            out.print("</Value>");
            break;
        case DATETIME:
            out.print("<Value>");
            out.print(dateTimeFormat.format((Date)value));
            out.print("</Value>");
            break;
        case BLOB:
            out.print("<Value>");
            out.print(Base64Coder.encode((byte[])value));
            out.print("</Value>");
            break;
        case REFERENCE:
            OdmaObject referencedObject = (OdmaObject)value;
            OdmaClass referencedObjectClass = referencedObject.getOdmaClass();
            String referencedObjectId = referencedObject.getId().toString();
            if(referencedObjectClass.getNamespace().equals("opendma"))
            {
                // we can reference all OpenDMA classes without the need to export the referenced class
                out.print("<Value>");
                out.print(referencedObjectId);
                out.print("</Value>");
                break;
            }
            String referenceIdToBeWritten = null;
            if(isReferenceExported(referencedObject))
            {
                if(!exportedObjects.containsKey(referencedObjectId))
                {
                    if(isNotRetrievable(referencedObject))
                    {
                        if(!nonRetrievableObjects.containsKey(referencedObjectId))
                        {
                            nonRetrievableObjects.put(referencedObjectId, referencedObject);
                        }
                    }
                    else
                    {
                        exportQueue.put(referencedObjectId, ((OdmaObject)value).getOdmaClass().getQName());
                    }
                }
                referenceIdToBeWritten = referencedObjectId;
            }
            if(referenceIdToBeWritten != null)
            {
                out.print("<Value>");
                out.print(referenceIdToBeWritten);
                out.print("</Value>");
            }
            break;
        case CONTENT:
            if(exportContent)
            {
                String filename = contentDirectory+"/content"+Integer.toString(this.exportetContentIdCounter++)+".dat";
                File dataDir = new File(contentDirectory);
                if(!dataDir.exists())
                {
                    if(!dataDir.mkdirs())
                    {
                        throw new RuntimeException("Error creating directory for content data files.");
                    }
                }
                try
                {
                    InputStream inContent = ((OdmaContent)value).getStream();
                    try
                    {
                        FileOutputStream fos = new FileOutputStream(filename);
                        try
                        {
                            byte[] buffer = new byte[1024];
                            int num = 0;
                            while((num = inContent.read(buffer)) > 0)
                            {
                                fos.write(buffer,0,num);
                            }
                        }
                        finally
                        {
                            fos.close();
                        }
                    }
                    finally
                    {
                        inContent.close();
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Error exporting content into data file.",e);
                }
                out.print("<Value>");
                out.print(filename);
                out.print("</Value>");
            }
            break;
        case ID:
            out.print("<Value>");
            out.print(((OdmaId)value).toString());
            out.print("</Value>");
            break;
        case GUID:
            throw new RuntimeException("GUID propertys should have been omited. Property: "+propQName);
        default:
            throw new RuntimeException("Unhandled property type "+type);
        }
    }

    protected boolean isReferenceExported(OdmaObject referencedObject)
    {
        String id = referencedObject.getId().toString();
        if(excludeIds.contains(id))
        {
            return false;
        }
        String classQName = referencedObject.getOdmaClass().getQName().toString();
        for(int i = 0; i < excludeClasses.size(); i++)
        {
            if(excludeClasses.get(i).matcher(classQName).matches())
            {
                return false;
            }
        }
        return true;
    }

    public void dumpXMLString(PrintStream out, String s)
    {
        for(int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);
            switch(c)
            {
            case '"':
                out.print("&#x0022;");
                break;
            case '&':
                out.print("&#x0026;");
                break;
            case '\'':
                out.print("&#x0027;");
                break;
            case '<':
                out.print("&#x003C;");
                break;
            case '>':
                out.print("&#x003E;");
                break;
            default:
                out.print(c);
                break;
            }
        }
    }

    protected boolean isNotRetrievable(OdmaObject referencedObject)
    {
        return !referencedObject.getOdmaClass().isRetrievable();
    }
    
    protected static DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static Map<Integer, String> datatypeValues = new HashMap<Integer, String>();
    
    static
    {
        datatypeValues.put(new Integer(1),"string");
        datatypeValues.put(new Integer(2),"integer");
        datatypeValues.put(new Integer(3),"short");
        datatypeValues.put(new Integer(4),"long");
        datatypeValues.put(new Integer(5),"float");
        datatypeValues.put(new Integer(6),"double");
        datatypeValues.put(new Integer(7),"boolean");
        datatypeValues.put(new Integer(8),"datetime");
        datatypeValues.put(new Integer(9),"blob");
        datatypeValues.put(new Integer(10),"reference");
        datatypeValues.put(new Integer(11),"content");
        datatypeValues.put(new Integer(100),"id");
    }

    private void dumpClassTreeIter(PrintStream out, OdmaClass cls) throws Exception
    {
        if(!cls.getNamespace().equals("opendma"))
        {
            if(verbose > 0)
            {
                System.out.println("Processing class "+cls.getQName());
            }
            // write this Class if it has not yet been exported. If the class is not retrievable, it might already have been written with a referencing object
            if(!exportedObjects.containsKey(cls.getId().toString()))
            {
                dumpObject(out,cls);
            }
            // dump declared properties
            for(OdmaPropertyInfo pi : cls.getDeclaredProperties())
            {
                if(verbose > 0)
                {
                    System.out.println("    Processing property "+pi.getQName());
                }
                // write this PropertyInfo if it has not yet been exported. If the class is not retrievable, it might already have been written with a referencing object
                if(!exportedObjects.containsKey(pi.getId().toString()))
                {
                    dumpObject(out,pi);
                }
            }
        }
        // dump sub classes
        for(OdmaClass subClass : cls.getSubClasses())
        {
            dumpClassTreeIter(out,subClass);
        }
    }

}
