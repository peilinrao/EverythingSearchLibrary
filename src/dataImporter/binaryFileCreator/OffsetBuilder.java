package dataImporter.binaryFileCreator;

/*
 * - findByteForBcolsBasedOnIndex(int index): int
 * 		For given Bcols index, return the amount of Bytes the info will requires
 * - public String getColumnTypeForBcols(int bcolsIndex): String
 * 		For given Bcols index, return the data type of that value, in the form of String
 * - getTotalSizeOfBlock(): int
 * 		Return the total size of the block to help in generating the offset of the end of the block, since we find the offset of an element in bcols going from right to left (decrementing i) in an ArrayList
 */

public class OffsetBuilder {
	
	public int findByteForBcolsBasedOnIndex(int index)
	{
		return 0;
	}
	
	public String getColumnTypeForBcols(int bcolsIndex)
	{
		return null;
	}
	
	public int getTotalSizeOfBlock()
	{
		return 0;
	}

}
