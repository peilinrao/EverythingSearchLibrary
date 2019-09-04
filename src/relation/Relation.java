package relation;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import dataImporter.binaryFileCreator.BinaryFileCreator;

/*
 * Attributes:
 * 	String filePathExtended;
	boolean columnStore;
	int headerBytes;
	ArrayList<Character> columnDataType
 * Methods:
 * - distinctValuesOfFirstColumn(): ArrayList<Object>
 * 		Get the distinct values of the first columns
 * - locationOfTuples(Object val): int
 * 		Gets the location(offset) of the value in first column. If not then raise error "ValueNotFoundException"
 * - jump(Object val): void
 * 		Jumps based on the value. Either it points to the specific value or the value greater than the specified value
 * - resetPointer(): void
 * 		Resets the pointer to the point where the header ends
 * - isMapEmpty(): boolean
 * 		If more tuples are available or not
 * - findFrequencyAndValueForGivenOffset(int offset): ArrayList<Object, Integer>
 * 		For a certain offset, returns the frequency and value.
 * - currentPointerCountAndValue(): ArrayList<Object, Integer>
 * 		Value and Count of first column where the cursor is currently present
 * - next(): If columnStore==true, 
				then next value
			 else
				then next tuple
 * - subRelation(): void
 * 		Performs sub-relation
 */

public class Relation extends BinaryFileCreator{

	String filePathExtended;
	int noOfColumnStores;
	boolean columnStore;
	int headerBytes;
	ArrayList<Character> columnDataType = new ArrayList<Character>();
	
	public Relation(String filePathExtended, boolean columnStore, int headerBytes,ArrayList<Character> columnDataType, int noOfColumnStores)
	{
		super();
		this.filePathExtended = filePathExtended;
		this.columnStore = columnStore;
		this.headerBytes = headerBytes;
		this.columnDataType = columnDataType;
		this.noOfColumnStores = noOfColumnStores;
	}
	
	public String convertCurrentPointingByteToString(DataInputStream dis) throws IOException
	{
		byte[] str = " ".getBytes();
		str[0] = dis.readByte();
		String string = new String(str, "ASCII");
		return string;
	}
	
	public String findStringFromGivenOffsetAndDelimeter(int offset, String delimeter, DataInputStream dis) throws IOException
	{
		if(offset!=-1)//Ignore offset
			dis.skipBytes(offset-1);
		String value = "";

		while(true)
		{
			String string = convertCurrentPointingByteToString(dis);

			if(delimeter.contains(string))
				break;
			else
				value+=string;
		}
//		System.out.println("findStringFromGivenOffsetAndDelimeter value:"+value);
		return value;
	}
	
	public int findIntegerFromGivenOffsetAndDelimeter(int offset, DataInputStream dis) throws IOException
	{
		if(offset!=-1)//Ignore offset
			dis.skipBytes(offset-1);
		int value = dis.readInt();
		return value;
	}
	
	//Moves the dis object to the location of the next tuple
	public void next(DataInputStream dis) throws IOException
	{
		if(this.columnStore)
		{
			if(columnDataType.get(0).equals('s'))
			{
				findStringFromGivenOffsetAndDelimeter(-1, "_", dis);
				dis.skipBytes(intInBytes+intInBytes);
			}
			else
			{
				findIntegerFromGivenOffsetAndDelimeter(-1, dis);
				dis.skipBytes(intInBytes+intInBytes);
			}
		}
		else
		{
			String string = convertCurrentPointingByteToString(dis);
			if(string.equals(","))
			{
				findStringFromGivenOffsetAndDelimeter(-1, "_", dis);
				dis.skipBytes(intInBytes);
			}
			else
			{
				findIntegerFromGivenOffsetAndDelimeter(-1, dis);
				dis.skipBytes(charInBytes+intInBytes);
			}
		}
	}
		
	public ArrayList<Object> currentPointerCountValueAndOffset(DataInputStream dis) throws IOException
	{
		ArrayList<Object> list = new ArrayList<Object>();
		dis.mark(50);
		
		if(this.columnStore)
		{
			if(this.columnDataType.get(0).equals('s'))
				list.add(findStringFromGivenOffsetAndDelimeter(-1, "_", dis));
			else
				list.add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));
			list.add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));//Value
			list.add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));//Offset
		}
		else
		{
			String string = convertCurrentPointingByteToString(dis);
			if(string.equals(","))
				list.add(findStringFromGivenOffsetAndDelimeter(-1, "_", dis));
			else
				list.add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));
			list.add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));//Value
			list.add(null);//Offset
		}
		dis.reset();
		return list;
	}
	
	public void readFromFile(String filePathExtended, boolean columnStore, char datatype) throws FileNotFoundException, IOException
	{
		DataInputStream dis = getDataInputStreamObject(filePathExtended);
		String value = "";
		System.out.println();
		System.out.print("readFromFile()  List:");
//		this.columnStore = columnStore;
//		this.columnDataType.add(datatype);
		if(columnStore)
		{
			ArrayList<Object> list = new ArrayList<Object>();
			while(dis.available()!=0)
			{
				list.addAll(currentPointerCountValueAndOffset(dis));
				next(dis);
			}
			displayArrayList(list);
		}
		else
		{
			System.out.print("[");
			while(dis.available()>0)
			{
				String string = convertCurrentPointingByteToString(dis);
				if(string.equals("_"))
				{
					if(value.length()>0)
					{
						System.out.print("," + value);
						value = "";
					}
					int integer = dis.readInt();
					System.out.print("_"+ integer);
				}
				else if(string.equals(","))
				{
					continue;
				}
				else if(string.equals(" "))
				{
					System.out.print(", ");
					value = "";
				}
				else if(string.equals(";"))
				{
					System.out.print(",;");
					value = "";
				}
				else	
				{
					value += string;
				}
			}
			System.out.print("]");
		}
	}
	
	public ArrayList<ArrayList<Object>> distinctValuesOfFirstColumn() throws Exception
	{
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		for(int i=0; i<this.columnDataType.size();i++)
		{
			list.add(new ArrayList<Object>());
		}
		DataInputStream dis = getDataInputStreamObject(this.filePathExtended);
		if(this.headerBytes>0)
			dis.skipBytes(this.headerBytes-1);
		if(this.columnStore)
		{
			while(dis.available()!=0)
			{
				list.get(0).add(currentPointerCountValueAndOffset(dis).get(0));
				next(dis);
			}
//			displayArrayList(list);
			return list;
		}
		else
		{
			int counterForList = 0;
			while(dis.available()>0)
			{
				System.out.println("distinctValuesOfFirstColumn counterForList:"+counterForList);
				String string = convertCurrentPointingByteToString(dis);
				System.out.println("distinctValuesOfFirstColumn string:"+string);
				if(string.equals(","))
				{
					dis.mark(50);
					String str = findStringFromGivenOffsetAndDelimeter(-1, "_", dis);
					System.out.println("distinctValuesOfFirstColumn str:"+str);
					if(str.equals(";"))
					{
						counterForList = 0;
						dis.reset();
						dis.skipBytes(1);
					}
					else
					{
						list.get(counterForList++).add(str);
						dis.skipBytes(charInBytes+intInBytes);
					}
				}
				else
				{
					list.get(counterForList++).add(findIntegerFromGivenOffsetAndDelimeter(-1, dis));
					dis.skipBytes(charInBytes+intInBytes);
					displayArrayList(list);
				}
			}
			System.out.print("]");
			return list;
		}
	}
	
	public int locationOfTuples(Object val) throws NumberFormatException, IOException
	{
		DataInputStream dis = getDataInputStreamObject(this.filePathExtended);
		int lengthOfTheFile = dis.available();
		if(this.headerBytes>0)
			dis.skipBytes(this.headerBytes-1);
		if(this.columnStore)
		{
			while(dis.available()!=0)
			{
				System.out.println(this.columnDataType.get(0).equals('s'));
				int location = lengthOfTheFile-dis.available();
				if(this.columnDataType.get(0).equals('s'))
				{
					if((""+val).equals(currentPointerCountValueAndOffset(dis).get(0)))
						return location;
				}
				else
				{
					if((Integer.parseInt(""+val)==((int)currentPointerCountValueAndOffset(dis).get(0))))
						return location;	
				}
				next(dis);
			}
		}
		return -1;
	}
	
	public void jump(Object val, DataInputStream dis) throws NumberFormatException, IOException
	{
		if(this.columnStore)
		{
			while(dis.available()!=0)
			{
				if(this.columnDataType.get(0).equals('s'))
				{
					if((""+val).equals(currentPointerCountValueAndOffset(dis).get(0)))
						break;
				}
				else
				{
					if((Integer.parseInt(""+val)<=((int)currentPointerCountValueAndOffset(dis).get(0))))
						break;	
				}
				next(dis);
			}
		}
	}
	
	public boolean areThereAnyMoreTuples(DataInputStream dis) throws IOException
	{
		return dis.available()!=0;
	}
	
	public void resetPointer(DataInputStream dis) throws Exception
	{
		dis = getDataInputStreamObject(this.filePathExtended);
		if(this.headerBytes>0)
			dis.skipBytes(this.headerBytes-1);
	}
	
	public Relation subRelation()
	{
		boolean columnStoreSubRel;
		String filePathExtendedSubRel;
		if(this.noOfColumnStores==1)
		{
			columnStoreSubRel = false;
			filePathExtendedSubRel = "src/RowStore.bin";
		}
		else
		{
			columnStoreSubRel = true;
			filePathExtendedSubRel = "src/ColumnStore("+ (Integer.parseInt(""+this.filePathExtended.charAt(this.filePathExtended.length()-6))+1) + ").bin";
		}
		int noOfColumnStoresSubRel = this.noOfColumnStores-1;
		int headerBytesSubRel = 0;
		ArrayList<Character> columnDataTypeSubRel = new ArrayList<Character>(this.columnDataType);columnDataTypeSubRel.remove(0);
		
		return new Relation(filePathExtendedSubRel, columnStoreSubRel, headerBytesSubRel, columnDataTypeSubRel, noOfColumnStoresSubRel);
	}
	
	public void theBrainOfRelation()
	{
		try 
		{
			ArrayList<ArrayList<Object>> list = distinctValuesOfFirstColumn();
			if(this.columnStore)
				displayArrayList(list.get(0));
			else
			{
				for(int i=0;i<list.size();i++)
				{
					displayArrayList(list.get(i));
				}
			}
			
			System.out.println(locationOfTuples("g"));
			
			System.out.println("subRelation()"+subRelation().filePathExtended);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		Relation objOfRelation = new Relation("src/ColumnStore(1).bin", true, 0, new ArrayList<Character>(Arrays.asList('s', 's', 's', 'i', 'i', 'i')), 3);
		objOfRelation.theBrainOfRelation();
		
//		Relation objOfRelation2 = new Relation("src/RowStore.bin", true, 0, new ArrayList<Character>(Arrays.asList('i', 'i', 'i')));
//		objOfRelation2.theBrainOfRelation();
	}
}
