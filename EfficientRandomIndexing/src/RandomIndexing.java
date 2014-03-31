import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.la4j.vector.sparse.CompressedVector;
/**
 * This calss is a multithreaded document-based implementation of Random Indexing. 
 * Being document-based means that it is based on a word-document coocuurance matrix so
 * we build a random index for each document (context) and add it to the words that occur within
 * that document.
 * For word-word Random Indexing some modifications is needed.
 * @author af
 *
 */
public class RandomIndexing {

    public static void main(String args[]) throws InterruptedException, IOException{

    	 Logger logger = Logger.getLogger("RandomIndexing");
    	 logger.info("Usage: RandomIndexing inputfile numThreads vocabfile");
    	 logger.info("input file: " + args[0]);
    	 
    	 long start = System.currentTimeMillis();
    	//Creating shared object
	     BlockingQueue<TextContextPair> sharedQueue = new LinkedBlockingQueue<TextContextPair>(10000);
	     ConcurrentHashMap<String, CompressedVector> wordVectors = new ConcurrentHashMap<String, CompressedVector>();
	     ConcurrentHashMap<String, Lock> locks = new ConcurrentHashMap<String, Lock>();
	     int dimension = 2048;
	     int nonZero = 7;
	     //Creating Producer and Consumer Thread
	     int numConsumerThreads = Integer.parseInt(args[1]);
	     String vocabFile = null;
	     if(args.length == 3){
	    	 vocabFile = args[2];
	     }
	     List<Thread> consumerThreads = new ArrayList<Thread>(numConsumerThreads);
	     for(int i = 0;i < numConsumerThreads; i++){
	    	 Thread consThread = new Thread(new Consumer("" + i, sharedQueue, locks, wordVectors, vocabFile));
	    	 consumerThreads.add(consThread);
	    	 consThread.setDaemon(true);
	    	 consThread.start();
	     }
	     Thread prodThread = new Thread(new Producer(args[0], sharedQueue, dimension, nonZero));
	     
	
	     //Starting producer and Consumer thread
	     prodThread.start();
	     
	     //wait untill producer finishes
	     while(!Producer.finished){
	    	 Thread.sleep(60000);
	     }
	     //now wait untill job queue is empty
	     while(!sharedQueue.isEmpty()){
	    	 Thread.sleep(60000);
	     }
	     //now producers and consumers have hopefully finished their work. stop all threads
	    Consumer.finishWork = true;
	    //wait a little more for any unfinished buisiness
	    Thread.sleep(60000);
	    //now all producer and consumer threads are dead. start saving the vectors.
	    Iterator iter = wordVectors.entrySet().iterator();
	    File file = new File("random-index.vectors.txt");
	    BufferedWriter bw = new BufferedWriter(new FileWriter(file));
	    
	    logger.info("writing random indices to file...");
	    int i = 0;
	    while(iter.hasNext()){
	    	i++;
	    	if(i%10000 == 0){
	    		logger.info("writing " + i + "th line to file");
	    	}
	    	Entry<String, CompressedVector> entry = (Entry<String, CompressedVector>) iter.next();
	    	String word = entry.getKey();
	    	CompressedVector wordVector = entry.getValue();
	    	//convert wordVector to a textual representation.
	    	//optimization point: use StringBuilder
	    	String result = word + "\t" + wordVector.toString() + "\n";
	    	bw.write(result);
	    	//write a line in a file
	    }
	    bw.flush();
	    bw.close();
	    logger.info("finished writing random indices to file: " + file.getAbsolutePath());
    }
 
}

//Producer Class in java
class Producer implements Runnable {

	private Logger logger = Logger.getLogger(Producer.class.getName());
	private final BlockingQueue<TextContextPair> sharedQueue;
	private final int dimension;
	private final int nonZero;
	public static boolean finished = false;
	private String fileAddress;

    public Producer(String fileAddress, BlockingQueue sharedQueue, int dimension, int nonZero) {
        this.sharedQueue = sharedQueue;
        this.dimension = dimension;
        this.nonZero = nonZero;
        this.fileAddress = fileAddress;
        
    }
    
    

    @Override
    public void run() {
        //reading file line by line in Java using BufferedReader       
    	try {
            
        int docNumber = -1;   
    	FileInputStream fis = null;
        BufferedReader reader = null;
        logger.log(Level.INFO, "producer started working.");
        try {
            fis = new FileInputStream(fileAddress);
            reader = new BufferedReader(new InputStreamReader(fis));
            String line = reader.readLine();
            while(line != null){
            	docNumber++;
            	if(docNumber % 10000 == 0){
            		logger.info("processing line: " + docNumber);
            		
            	}
            	//initialize a context vector for this line/document
            	CompressedVector docVector = creatDocVector(docNumber);
            	System.out.println(docVector.toString());
            	TextContextPair pair = new TextContextPair(docVector, line);
            	//System.out.println(line);
                sharedQueue.put(pair);
                line = reader.readLine();
            }
            
          
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
          
        } finally {
            try {
                reader.close();
                fis.close();
            } catch (IOException ex) {
                Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
  


    
    	} catch (InterruptedException ex) {
                Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.finished = true;
        logger.log(Level.INFO, "prducer finished her job.");
        logger.log(Level.INFO, "queue size is " + sharedQueue.size());
    }

	private CompressedVector creatDocVector(int docNumber) {
		// TODO Auto-generated method stub
		//create nonZero indices
		int [] indices = new int[this.nonZero];
		//create nonZero values
		double [] values = new double[this.nonZero];
		Random random = new Random(docNumber);
		for(int i = 0; i< this.nonZero; i++){
			indices[i] = random.nextInt(this.dimension);
			boolean value = random.nextBoolean();
			if(value){
				values[i] = 1;
			}else{
				values[i] = -1;
			}
		}
		//create sparse vector and add the values
		return new CompressedVector(this.dimension, this.nonZero, values, indices);
		
	}

}

//Consumer Class in Java
class Consumer implements Runnable{
	private Logger logger = Logger.getLogger(this.getClass().getName());
    private final BlockingQueue<TextContextPair> sharedQueue;
    private final ConcurrentHashMap<String, Lock> locks;
    private final ConcurrentHashMap<String, CompressedVector> words;
    public static boolean finishWork = false;
    public String name;
    public int numDone;
    public HashSet<String> stopWords = null;
    public HashSet<String> vocabulary = null;
    

    public Consumer (String name, BlockingQueue<TextContextPair> sharedQueue, ConcurrentHashMap<String, Lock> locks, 
    		ConcurrentHashMap<String, CompressedVector> words, String vocabularyFile) {
        this.sharedQueue = sharedQueue;
        this.locks = locks;
        this.words = words;
        this.name = name;
        if (vocabularyFile != null){
        	this.vocabulary = loadVocab(vocabularyFile);
        }
        
    }
  
    private HashSet<String> loadVocab(String vocabularyFile) {
		// TODO Auto-generated method stub
    	HashSet<String> vocab = new HashSet<String>();
    	FileInputStream fis = null;
        BufferedReader reader = null;
        logger.log(Level.INFO, "producer started working.");
        try {
            fis = new FileInputStream(vocabularyFile);
            reader = new BufferedReader(new InputStreamReader(fis));
            
            String line = reader.readLine();
            while(line != null){
                line = reader.readLine();
                String [] fields = line.split("\\s+");
                vocab.add(fields[0]);
            }
            
          
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
          
        } finally {
            try {
                reader.close();
                fis.close();
            } catch (IOException ex) {
                Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return vocab;
    }

	@Override
    public void run() {
        while(!Consumer.finishWork){
            try {
            	
                //System.out.println("Consumed: "+ sharedQueue.take());
                TextContextPair pair = sharedQueue.poll();
          
                if(pair == null){
                	//queue was empty wait 10 seconds and start again
                	Thread.sleep(10000);
                	continue;
                }
                numDone++;
                String text = pair.text;
                //tokenize the text using a simple whitespace tokenizer.
                //improvement point: use a tokenizer class (e.g. lucene tokenizer)
                String [] tokens = text.split("\\s+");
                //for each word retrieve the specific lock or create new if needed.
                for(String token: tokens){
                	//if vocabulary is not null and token is not in vocabulary do nothing.
                	if(this.vocabulary !=null && (!this.vocabulary.contains(token)) ){
                		continue;
                	}
                	Lock newLock = new ReentrantLock();
                	Lock keyLock = locks.putIfAbsent(token, newLock);
                	if(keyLock == null){
                		keyLock = newLock;
                	}
                	//lock words just for the specified key
                	keyLock.lock();
                	//retrieve the token vector if available. If not available create a new Empty vector
                	CompressedVector wordVector = words.get(token);
                	if(wordVector != null){
                		//add contextVector to wordVector
                		wordVector = (CompressedVector) wordVector.add(pair.contextVector);
                	}else{
                		//the first time we see the word
                		wordVector = pair.contextVector;
                	}
                	//put back the updated wordVector
                	words.put(token, wordVector);
                	//unlock this token
                	keyLock.unlock();
                	
                }
                
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        logger.log(Level.INFO, "Thread " + this.name + " finished his job.");
        logger.log(Level.INFO, "word vector size is " + words.size());
    }
  
  
}


