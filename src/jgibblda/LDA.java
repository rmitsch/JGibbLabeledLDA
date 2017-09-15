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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;

import org.kohsuke.args4j.*;

import tapas.DatabaseConnector;
import tapas.TopicModel;

public class LDA
{
    public static void main(String args[])
    {
//    	CONTINUE:
//		- drop foreign keys before bulk import (Ref.class time: ~ 10 mins))
//		- import other topic data
//		- introduce multithreading
//		- incorporate in python part
//		- rewrite python part (facets, calling java etc.)

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
            LDADataset data = new LDADataset(option, true);

            if (option.est || option.estc){
                Estimator estimator = new Estimator(option, dbConnector, data);
                estimator.estimate();
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            	Date date = new Date();
            	System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
            }
         // Disregard inferencer for now - not tested, hence not supported unless it becomes necessary for TAPAS.
//            else if (option.inf){
//                Inferencer inferencer = new Inferencer(option, dbConnector, data);
//                Model newModel = inferencer.inference();
//            }
        }

        catch (CmdLineException cle){
            System.out.println("Command line error: " + cle.getMessage());
            showHelp(parser);
            return;
        }

        catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        catch (Exception e){
            System.out.println("Error in main: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    public static void showHelp(CmdLineParser parser){
        System.out.println("LDA [options ...] [arguments...]");
        parser.printUsage(System.out);
    }

}
