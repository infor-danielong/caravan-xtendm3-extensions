/**
*  @Name: CREXTPRT.ExtendOutput
*  @Description: ExtendOutput for MWS435PF, MMS480PF/MWS620PF
*  @Authors: Nixon Power Ong
*/

/**
* CHANGELOGS
* Version    Date    User               Description
* 1.0.0      240315  Nixon Power Ong    Initial Release
*/

import java.util.*;
import java.util.regex.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ExtendOutput extends ExtendM3Trigger {
  private final MethodAPI method;
  private final ProgramAPI program;
  private final DatabaseAPI database;
  private final LoggerAPI logger;
  private final MICallerAPI miCaller;
  
  int MAX_RECORDS = 10000;
  
  public ExtendOutput(MethodAPI method, ProgramAPI program, DatabaseAPI database, LoggerAPI logger, MICallerAPI miCaller) {
	  this.program = program;
    this.database = database;
    this.method = method;
    this.logger = logger;
    this.miCaller = miCaller;
  }
  
  public void main() {
    String printerFile = method.getArgument(0) as String;
    String jobNumber = method.getArgument(1) as String;
    String structure = method.getArgument(2) as String;
    int variant = method.getArgument(3) as int;
    int rpbk = method.getArgument(4) as int;
    HashMap<String, Object> fieldMap = method.getArgument(5) as HashMap;
    
    logger.debug("@@ExtendOutput is running");

    if(printerFile.equals("MMS480PF") || printerFile.equals("MWS620PF")) {
      if (structure.equals("CRV_03-01")) {
        if (rpbk == 6) {
          startMMS480PFv3(fieldMap.get("OQCONO") as int,
                          fieldMap.get("OQINOU") as int,
                          fieldMap.get("OQCONN") as int,
                          fieldMap.get("&SIZE") as int);
        }
      }
    } else if(printerFile.equals("MWS435PF")) {
      if (structure.equals("CRV_01-01")) {
        if (rpbk == 6) {
          startMWS435PFv1(fieldMap.get("OQCONO") as int,
                          fieldMap.get("OQINOU") as int,
                          fieldMap.get("OQCONN") as int,
                          fieldMap.get("&SIZE") as int);
        }
      }
    }
  }
  
  /**
   * Extend output for MWS435PF variant 1
   * @params CONO - Input company
   * @params INOU - Input direction
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
   */
  void startMWS435PFv1(int CONO, int INOU, int CONN, int ROW_SIZE) {
    deleteRecordsByCONN(CONO, CONN, "EXT434");
    deleteRecordsByCONN(CONO, CONN, "EXT435");
    deleteRecordsByCONN(CONO, CONN, "EXT436");
    
    DBAction query = database.table("MHDISH").index("20").selection("OQCONO", "OQINOU", "OQCONN", "OQDLIX", "OQCONA").build();
    DBContainer container = query.createContainer();
    container.set("OQCONO", CONO);
    container.set("OQINOU", INOU);
    container.set("OQCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    DBAction MHDISL_query = database.table("MHDISL").index("00").selection("URCONO", "URDLIX", "URITNO", "URRIDN", "URRIDL").build();
    DBContainer MHDISL_container = MHDISL_query.createContainer();
    
    DBAction MITMAH_query = database.table("MITMAH").index("00").selection("HMTY15", "HMTX15").build();
    DBContainer MITMAH_container = MITMAH_query.createContainer();
    
    query.readAll(container, 3, MAX_RECORDS, { DBContainer MHDISH_data ->
      Map result = new HashMap(); 
      
      String CONA = MHDISH_data.get("OQCONA");
      
      MHDISL_container.set("URCONO", CONO);
      MHDISL_container.set("URDLIX", MHDISH_data.get("OQDLIX") as int);
      int ctr = 1;
      int pageNum = 1;
    
      MHDISL_query.readAll(MHDISL_container, 2, MAX_RECORDS, { DBContainer MHDISL_data ->
        Map result_Line = new HashMap(); 
        
        String ITNO = MHDISL_data.get("URITNO");
        String blockNo = getBlock(CONO, ITNO);
        String color = "";
        String size = "";
        
        MITMAH_container.set("HMCONO", CONO);
        MITMAH_container.set("HMITNO", ITNO);
        
        if (MITMAH_query.read(MITMAH_container)) {
          color = MITMAH_container.get("HMTY15");
          size = MITMAH_container.get("HMTX15");
        }
        
        result_Line.put("DLIX", MHDISH_data.get("OQDLIX") as String);
        result_Line.put("CONA", CONA.trim());
        result_Line.put("ORNO", MHDISL_data.get("URRIDN"));
        result_Line.put("PONR", MHDISL_data.get("URRIDL") as String);
        result_Line.put("ITNO", ITNO);
        result_Line.put("CFI4", blockNo);
        result_Line.put("COLR", color);
        result_Line.put("SIZE", size);
      
        if(blockNo.isNumber()) {
          int block = blockNo as int;
          
          if(block <= 3999) {
            result_Line.put("TYPE", "1");
          } else {
            result_Line.put("TYPE", "2");
          }
        } else {
            result_Line.put("TYPE", "0");
        }
        
        LINES.add(result_Line);
      });
      
    });
   
    insertDataToEXT436(CONO, CONN, LINES);
    processSortedData(CONO, CONN, ROW_SIZE);
  }
  
  /**
   * Process sorted data from EXT436 and compute page number
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
   */
  void processSortedData(int CONO, int CONN, int ROW_SIZE) {
    DBAction SORTED_query = database.table("EXT436").index("00").selectAllFields().build();
    DBContainer SORTED_container = SORTED_query.createContainer();
    SORTED_container.set("EXCONO", CONO);
    SORTED_container.set("EXCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    int ctr = 1;
    int pageNum = 1;
    
    String currCONA = null;
    int currType = -1;
    
    DBAction LINE_query = database.table("EXT435").index("00").build();
    DBContainer LINE_container = LINE_query.createContainer();
    LINE_container.set("EXCONO", CONO);
    LINE_container.set("EXCONN", CONN);
    
    DBAction HEAD_query = database.table("EXT434").index("00").build();
    DBContainer HEAD_container = HEAD_query.getContainer();
    HEAD_container.set("EXCONO", CONO);
    HEAD_container.set("EXCONN", CONN);
      
    SORTED_query.readAll(SORTED_container, 2, MAX_RECORDS, { DBContainer SORTED_data ->
      String CONA = SORTED_data.get("EXCONA");
      int TYPE = SORTED_data.get("EXTYPE");
      
      if(currCONA == null && currType == -1) {
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(!currCONA.equals(CONA) || (currCONA.equals(CONA) && currType != TYPE)) {
        ctr = 1;
        pageNum = 1;
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(ctr > ROW_SIZE) {
        ctr = 1;
        pageNum++;
      }
      
      LINE_container.set("EXORNO", SORTED_data.get("EXORNO"));
      LINE_container.set("EXPONR", SORTED_data.get("EXPONR"));
      LINE_container.set("EXDLIX", SORTED_data.get("EXDLIX"));
      LINE_container.set("EXCONA", SORTED_data.get("EXCONA"));
      LINE_container.set("EXITNO", SORTED_data.get("EXITNO"));
      LINE_container.set("EXCFI4", SORTED_data.get("EXCFI4"));
      LINE_container.set("EXTYPE", SORTED_data.get("EXTYPE"));
      LINE_container.set("EXCOLR", SORTED_data.get("EXCOLR"));
      LINE_container.set("EXSIZE", SORTED_data.get("EXSIZE"));
      LINE_container.set("EXPAGE", pageNum);
      
      LINE_container.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      LINE_container.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXCHNO", 0);
      LINE_container.set("EXCHID", program.getUser());
      
      HEAD_container.set("EXCONA", SORTED_data.get("EXCONA"));
      HEAD_container.set("EXTYPE", SORTED_data.get("EXTYPE"));
      HEAD_container.set("EXPAGE", pageNum);
      
      HEAD_container.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      HEAD_container.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      HEAD_container.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      HEAD_container.set("EXCHNO", 0);
      HEAD_container.set("EXCHID", program.getUser());
      
      LINE_query.insert(LINE_container);
      HEAD_query.insert(HEAD_container);
      
      ctr++;
    });
  }
  
  /**
   * Insert data to the EXT436 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
   */
  void insertDataToEXT436(int CONO, int CONN, ArrayList<Map<String, String>> LINES) {
    DBAction LINE_query = database.table("EXT436").index("00").build();
    DBContainer LINE_container = LINE_query.getContainer();
    LINE_container.set("EXCONO", CONO);
    LINE_container.set("EXCONN", CONN);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      LINE_container.set("EXDLIX", record.DLIX as int);
      LINE_container.set("EXCONA", record.CONA);
      LINE_container.set("EXORNO", record.ORNO);
      LINE_container.set("EXPONR", record.PONR as int);
      LINE_container.set("EXITNO", record.ITNO);
      LINE_container.set("EXCFI4", record.CFI4);
      LINE_container.set("EXTYPE", record.TYPE as int);
      LINE_container.set("EXCOLR", record.COLR);
      LINE_container.set("EXSIZE", record.SIZE);
      
      LINE_container.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      LINE_container.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXCHNO", 0);
      LINE_container.set("EXCHID", program.getUser());
      
      LINE_query.insert(LINE_container);
    }
  }
  
  /**
   * Get MMCFI4 - Block number from MITMAS
   * @params CONO - Input company
   * @params ITNO - Input item number
   * @return block
   */
  String getBlock(int CONO, String ITNO) {
    DBAction query = database.table("MITMAS").index("00").selection("MMCFI4").build();
    DBContainer container = query.createContainer();
    container.set("MMCONO", CONO);
    container.set("MMITNO", ITNO);
    
    String block = "";
    
    if (query.read(container)) {
      block = container.get("MMCFI4");
    }
    
    return block;
  }
  
  /**
   * Extend output for MMS480PF variant 3
   * @params CONO - Input company
   * @params INOU - Input direction
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
   */
  void startMMS480PFv3(int CONO, int INOU, int CONN, int ROW_SIZE) {
    deleteRecordsByCONN(CONO, CONN, "EXT479");
    deleteRecordsByCONN(CONO, CONN, "EXT480");
    
    DBAction query = database.table("MHDISH").index("20").selection("OQCONO", "OQINOU", "OQCONN", "OQDLIX", "OQCONA").build();
    DBContainer container = query.createContainer();
    container.set("OQCONO", CONO);
    container.set("OQINOU", INOU);
    container.set("OQCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    DBAction MHDISL_query = database.table("MHDISL").index("00").selection("URCONO", "URDLIX", "URITNO", "URRIDN", "URRIDL").build();
    DBContainer MHDISL_container = MHDISL_query.createContainer();
    
    query.readAll(container, 3, MAX_RECORDS, { DBContainer MHDISH_data ->
      Map result = new HashMap(); 
      
      String CONA = MHDISH_data.get("OQCONA");
      
      MHDISL_container.set("URCONO", CONO);
      MHDISL_container.set("URDLIX", MHDISH_data.get("OQDLIX") as int);
      int ctr = 1;
      int pageNum = 1;
    
      MHDISL_query.readAll(MHDISL_container, 2, MAX_RECORDS, { DBContainer MHDISL_data ->
        Map result_Line = new HashMap(); 
        
        if(ctr > ROW_SIZE) {
          ctr = 1;
          pageNum++;
        }
        
        result_Line.put("DLIX", MHDISH_data.get("OQDLIX") as String);
        result_Line.put("CONA", CONA.trim());
        result_Line.put("ORNO", MHDISL_data.get("URRIDN"));
        result_Line.put("PONR", MHDISL_data.get("URRIDL") as String);
        result_Line.put("ITNO", MHDISL_data.get("URITNO"));
        result_Line.put("PAGE", pageNum);
        
        LINES.add(result_Line);
        
        ctr++;
      });
      
    });
   
    insertDataToEXT480(CONO, CONN, LINES);
  }
  
  /**
   * Check CONA-Consignee if starts with 2301 or 2302
   * @params CONA - Input consignee
   * @params CONN - Input shipment number
   * @return boolean
   */
  boolean checkCONA(String CONA) {
    if(CONA.length() >= 4) {
      char[] ch = CONA.toCharArray();
      if(ch[0] == '2' && ch[1] == '3' && ch[2] == '0' && (ch[3] == '1' || ch[3] == '2')) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
  
  /**
   * Insert data to the EXT480 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
   */
  void insertDataToEXT480(int CONO, int CONN, ArrayList<Map<String, String>> LINES) {
    DBAction LINE_query = database.table("EXT480").index("00").build();
    DBContainer LINE_container = LINE_query.getContainer();
    LINE_container.set("EXCONO", CONO);
    LINE_container.set("EXCONN", CONN);
    
    DBAction HEAD_query = database.table("EXT479").index("00").build();
    DBContainer HEAD_container = HEAD_query.getContainer();
    HEAD_container.set("EXCONO", CONO);
    HEAD_container.set("EXCONN", CONN);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      LINE_container.set("EXDLIX", record.DLIX as int);
      LINE_container.set("EXCONA", record.CONA);
      LINE_container.set("EXPAGE", record.PAGE as int);
      LINE_container.set("EXORNO", record.ORNO);
      LINE_container.set("EXPONR", record.PONR as int);
      LINE_container.set("EXITNO", record.ITNO);
      
      LINE_container.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      LINE_container.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      LINE_container.set("EXCHNO", 0);
      LINE_container.set("EXCHID", program.getUser());
      
      HEAD_container.set("EXDLIX", record.DLIX as int);
      HEAD_container.set("EXCONA", record.CONA);
      HEAD_container.set("EXPAGE", record.PAGE as int);
      
      HEAD_container.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      HEAD_container.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      HEAD_container.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      HEAD_container.set("EXCHNO", 0);
      HEAD_container.set("EXCHID", program.getUser());
      
      if(checkCONA(record.CONA)) {
        LINE_container.set("EXTYPE", 1);
        LINE_container.set("EXAMNT", 99.99);
        HEAD_container.set("EXTYPE", 1);
        
        LINE_query.insert(LINE_container);
        HEAD_query.insert(HEAD_container);
      } else {
        LINE_container.set("EXTYPE", 1);
        LINE_container.set("EXAMNT", 99.99);
        HEAD_container.set("EXTYPE", 1);
        
        LINE_query.insert(LINE_container);
        HEAD_query.insert(HEAD_container);
        
        LINE_container.set("EXTYPE", 2);
        LINE_container.set("EXAMNT", 0);
        HEAD_container.set("EXTYPE", 2);
        
        LINE_query.insert(LINE_container);
        HEAD_query.insert(HEAD_container);
        
        LINE_container.set("EXTYPE", 3);
        LINE_container.set("EXAMNT", 99.99);
        HEAD_container.set("EXTYPE", 3);
        
        LINE_query.insert(LINE_container);
        HEAD_query.insert(HEAD_container);
      }
     
    }
  }
  
  /**
   * Delete all records depending on CONN
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params table - Input Xtend Table
   * @return nothing
   */
  void deleteRecordsByCONN(int CONO, int CONN, String table) {
    DBAction query = database.table(table).index("00").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    
    Closure<?> deleteRecords = {LockedResult result ->
      result.delete();
    }
    
    query.readAllLock(container, 2, deleteRecords);
  }
}


















