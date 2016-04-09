package test.java.operation;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.util.Random;

/**
 * Created by anshushukla on 02/01/16.
 */
public class Operations {
    static File inputFile;
    public static void main(String[] args) {



//        Floating point operations
//        doFloatOp(10);



//        XML parsing operations
//        inputFile = new File("src/test/java/operation/tempSAX.xml");
//
//        for(int i=0;i<10;i++)
//            doXMLparseOp(inputFile);







    }

    // check  floating pt operation
    public static void doFloatOp(long millisec) {
        long start=System.currentTimeMillis();
        while(System.currentTimeMillis()-start<=millisec) {
            long startTime = System.nanoTime();
            Random r = new Random(100000);
            float randNum = r.nextInt();
            randNum = randNum * randNum;
            System.out.println(randNum);
            long stopTime = System.nanoTime();
            System.out.println((stopTime - startTime) / (1000000.0));


        }
    }

    public static void doXMLparseOp(File inputFile) {



        long startTime = System.nanoTime();
//        XMLparse.readXMLDom();
//        XMLparse.readXmlSAX(inputFile);
        try {
//            if(new Random().nextInt()%2==0) {
//                System.out.println("even");
//                 inputFile = new File("src/test/java/operation/temp.xml");
//            }
//            else {
//                System.out.println("odd");
//                inputFile = new File("src/test/java/operation/tempSAX.xml");
//            }
//            inputFile = new File("src/test/java/operation/tempSAX.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            UserHandler userhandler = new UserHandler();


        ///////////////Open for actual
             saxParser.parse(inputFile, userhandler);


        } catch (Exception e) {
            e.printStackTrace();
        }
//        class UserHandler extends DefaultHandler {
//
//            boolean bFirstName = false;
//            boolean bLastName = false;
//            boolean bNickName = false;
//            boolean bMarks = false;
//
//            @Override
//            public void startElement(String uri,
//                                     String localName, String qName, Attributes attributes)
//                    throws SAXException {
//                if (qName.equalsIgnoreCase("student")) {
//                    String rollNo = attributes.getValue("rollno");
//                    System.out.println("Roll No : " + rollNo);
//                } else if (qName.equalsIgnoreCase("firstname")) {
//                    bFirstName = true;
//                } else if (qName.equalsIgnoreCase("lastname")) {
//                    bLastName = true;
//                } else if (qName.equalsIgnoreCase("nickname")) {
//                    bNickName = true;
//                }
//                else if (qName.equalsIgnoreCase("marks")) {
//                    bMarks = true;
//                }
//            }
//
//
//            public void endElement(String uri,
//                                   String localName, String qName) throws SAXException {
//                if (qName.equalsIgnoreCase("student")) {
//                    System.out.println("End Element :" + qName);
//                }
//            }
//
//            @Override
//            public void characters(char ch[],
//                                   int start, int length) throws SAXException {
//                if (bFirstName) {
//                    System.out.println("First Name: "
//                            + new String(ch, start, length));
//                    bFirstName = false;
//                } else if (bLastName) {
//                    System.out.println("Last Name: "
//                            + new String(ch, start, length));
//                    bLastName = false;
//                } else if (bNickName) {
//                    System.out.println("Nick Name: "
//                            + new String(ch, start, length));
//                    bNickName = false;
//                } else if (bMarks) {
//                    System.out.println("Marks: "
//                            + new String(ch, start, length));
//                    bMarks = false;
//                }
//            }
//        }
        long stopTime = System.nanoTime();
        System.out.println("time taken once (in millisec) - "+(stopTime - startTime)/(1000000.0));
    }



}
