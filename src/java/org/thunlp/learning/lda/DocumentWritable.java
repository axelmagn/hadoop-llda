package org.thunlp.learning.lda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DocumentWritable implements Writable {
  public int [] words = null;
  public int [] topics = null;
  public int [] labels = null;
  private int numWords = 0;
  private int numLabels = 0;
  private byte [] buffer = new byte[10240];
  
  public int getNumWords() {
    return numWords;
  }
  
  public void setNumWords( int n ) {
    if ( words == null || words.length < n ) {
      words = new int[n];
      topics = new int[n];
    }
    numWords = n;
  }

  public int getNumLabels() {
    return numLabels;
  }

  public void setNumLabels(int n) {
    if(labels == null || labels.length < n) {
      labels = new int[n];
    }
    numLabels = n;
  }

  public void readFields(DataInput input) throws IOException {
    int wordByteSize = input.readInt();
    int labelByteSize = input.readInt();
    int size = wordByteSize + labelByteSize;
    if (buffer.length < size) {
      buffer = new byte[size + 1024];
    }
    
    input.readFully(buffer, 0, size);
    setNumWords(wordByteSize / 4 / 2);
    for (int i = 0; i < numWords; i++) {
      words[i] = fourBytesToInt(buffer, i * 2 * 4);
      topics[i] = fourBytesToInt(buffer, (i * 2 + 1) * 4);
    }

    setNumLabels(labelByteSize / 4);
    for (int i = 0; i < numLabels; i++) {
      labels[i] = fourBytesToInt(buffer, wordByteSize + i * 4);
    }
  }

  public void write(DataOutput output) throws IOException {
    int wordByteSize = numWords * 2 * 4;
    int labelByteSize = numWords * 4;
    int size = wordByteSize + labelByteSize;
      if (buffer.length < size) {
        buffer = new byte[size + 1024];
      }
    for (int i = 0; i < numWords; i++) {
      intToFourBytes(buffer, i * 2 * 4, words[i]);
      intToFourBytes(buffer, (i * 2 + 1) * 4, topics[i]);
    }
    for(int i = 0; i < numLabels; i++) {
      intToFourBytes(buffer, wordByteSize + i * 2, labels[i]);
    }
    output.writeInt(wordByteSize);
    output.writeInt(labelByteSize);
    output.write(buffer, 0, size);
  }
  
  public static int fourBytesToInt(byte [] b, int offset) {
    int i = 
      (b[offset] << 24) + 
      ((b[offset + 1] & 0xFF) << 16) +
      ((b[offset + 2] & 0xFF) << 8) +
      (b[offset + 3] & 0xFF);
    return i;
  }

  public static void intToFourBytes(byte [] b, int offset, int i) {
    b[offset] = (byte) (i >>> 24);
    b[offset + 1] = (byte) (i >>> 16);
    b[offset + 2] = (byte) (i >>> 8);
    b[offset + 3] = (byte) (i);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for(int i = 0; i < numLabels; i++) {
      sb.append(i > 0 ? " " : "");
      sb.append(labels[i]);
    }
    sb.append("]\t");
    for ( int i = 0 ; i < numWords ; i++ ) {
      sb.append(i > 0 ? " " : "");
      sb.append(words[i]);
      sb.append(":");
      sb.append(topics[i]);
    }
    return sb.toString();
  }

}
