/*
 * Copyright (C) 2007 by
 *
 * 	Xuan-Hieu Phan
 *	hieuxuan@ecei.tohoku.ac.jp or pxhieu@gmail.com
 * 	Graduate School of Information Sciences
 * 	Tohoku University
 *
 *  Cam-Tu Nguyen
 *  ncamtu@gmail.com
 *  College of Technology
 *  Vietnam National University, Hanoi
 *
 * JGibbsLDA is a free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * JGibbsLDA is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JGibbsLDA; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */

package jgibblda;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.kohsuke.args4j.*;

import tapas.DatabaseConnector;

public class LDA
{
    public static void main(String args[])
    {
        LDACmdOption option = new LDACmdOption();
        CmdLineParser parser = new CmdLineParser(option);

        try {
            if (args.length == 0){
                showHelp(parser);
                return;
            }

            parser.parseArgument(args);

            // Build connection to database.
            DatabaseConnector dbConnector = new DatabaseConnector(option);

            // Load dataset.
            LDADataset data = new LDADataset();
            LDA.loadData(data, dbConnector, option);

            if (option.est || option.estc){
                Estimator estimator = new Estimator(option, dbConnector, data);
                estimator.estimate();
            }
            else if (option.inf){
            	// Disregard inferencer for now - not tested, hence not supported unless it becomes necessary for TAPAS.
//                Inferencer inferencer = new Inferencer(option, dbConnector, data);
//                Model newModel = inferencer.inference();
            }
        } catch (CmdLineException cle){
            System.out.println("Command line error: " + cle.getMessage());
            showHelp(parser);
            return;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (Exception e){
            System.out.println("Error in main: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    /**
     * Loads dataset from DB and updates options based on result.
     * @param data
     */
    private static void loadData(LDADataset data, DatabaseConnector dbConnector, LDACmdOption option)
    {
    	int topicModelID = Integer.parseInt(option.db_topic_model_id);

        try {
            // Read data set in specified database.
			data.readDataSet(topicModelID, dbConnector, option.unlabeled);
	        // After reading data set: Derive implicit (topic-model dependent) options.
	        option.deriveImplicitSettings(	dbConnector.extractTopicModel(topicModelID),
	        								DatabaseConnector.corpusFacetIDs_globalToLocal.size());
		}

        catch (FileNotFoundException e) {
			e.printStackTrace();
		}

        catch (IOException e) {
			e.printStackTrace();
		}
    }

    public static void showHelp(CmdLineParser parser){
        System.out.println("LDA [options ...] [arguments...]");
        parser.printUsage(System.out);
    }

}
