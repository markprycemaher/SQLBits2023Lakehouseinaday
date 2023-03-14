// Copyright 2020 Confluent Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Identity ;
using RandomNameGeneratorLibrary;

namespace CCloud
{
    class Program
    {
        static async Task<ClientConfig> LoadConfig(string configPath, string certDir)
        {
            try
            {
                var cloudConfig = (await File.ReadAllLinesAsync(configPath))
                    .Where(line => !line.StartsWith("#"))
                    .ToDictionary(
                        line => line.Substring(0, line.IndexOf('=')),
                        line => line.Substring(line.IndexOf('=') + 1));

                var clientConfig = new ClientConfig(cloudConfig);
                

                if (certDir != null)
                {
                    clientConfig.SslCaLocation = certDir;
                }

                return clientConfig;
            }
            catch (Exception e)
            {
                Console.WriteLine($"An error occured reading the config file from '{configPath}': {e.Message}");
                System.Environment.Exit(1);
                return null; // avoid not-all-paths-return-value compiler error.
            }
        }

        static async Task CreateTopicMaybe(string name, int numPartitions, short replicationFactor, ClientConfig cloudConfig)
        {
             Console.WriteLine("CreateTopicMaybe");
            using (var adminClient = new AdminClientBuilder(cloudConfig).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                        new TopicSpecification { Name = name, NumPartitions = numPartitions, ReplicationFactor = replicationFactor } });
                }
                catch (CreateTopicsException e)
                {
                    if (e.Results[0].Error.Code != ErrorCode.TopicAlreadyExists)
                    {
                        Console.WriteLine($"An error occured creating topic {name}: {e.Results[0].Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine("Topic already exists");
                    }
                }
            }
        }
        


        public class player
        {
            public string Name;
            public string Class;
            public string Race;
            public string Location;
            public int Id;
            public int Birthday;

            public double luck;
        }

        static public  List<player> GetPlayersList(int pcount)
        {
            List<player>  rt  = new List<player> ();
            Random rnd = new Random();
            int[] myDice = {4, 6, 8, 12, 20};
            string[] myClasses= {"Barbarian","Bard","Cleric","Druid","Fighter","Monk","Paladin","Ranger","Rogue", "Sorcerer","Warlock","Wizard"};
            string[] myRaces = { "Dragonborn","Dwarf","Elf","Gnome","Half-Elf","Half-Orc","Halfling","Human","Tiefling"};
            var personGenerator = new PersonNameGenerator();
            var placeGenerator = new PlaceNameGenerator();

          player p1 = new player();
          p1.Name = "Mark";
          p1.Class = "Working";
          p1.Race = "Human";
          p1.Location = "Norwich";
          p1.Id = 4;
          p1.Birthday = 1;
          p1.luck = 0.9;

            player p2 = new player();
          p2.Name = "Simon";
          p2.Class = "Sorcerer";
          p2.Race = "Dragonborn";
          p2.Location = "London";
          p2.Id = 3;
          p2.Birthday = 2;
          p2.luck = 1.5;

            player p3 = new player();
          p3.Name = "Stijn";
          p3.Class = "Ranger";
          p3.Race = "Tiefling";
          p3.Location = "Vorselaar";
          p3.Id = 2;
          p3.Birthday = 3;
          p3.luck = 1.6;
          
            player p4 = new player();
          p4.Name = "Filip";
          p4.Class = "Cleric";
          p4.Race = "Gnome";
          p4.Location = "Serbia";
          p4.Id = 1;
          p4.Birthday = 4;
          p4.luck = 1.4;

            rt.Add(p1);
            rt.Add(p2);
            rt.Add(p3);
            rt.Add(p4);

            double  luckcalc =  Convert.ToDouble (rnd.Next(0,500)) / 100.0; 
            int i=5; 



            while(i < pcount )
            {
                            var theClass = myClasses[rnd.Next(myClasses.Length)];
    var theRace = myRaces[rnd.Next(myRaces.Length)];
    var name = personGenerator.GenerateRandomFirstAndLastName();
    var place = placeGenerator.GenerateRandomPlaceName();
                        luckcalc =  Convert.ToDouble (rnd.Next(0,500)) / 1000.0;
                        player p = new player();
                        p.Id = i;
                        p.Name = name;
                        p.Location = place;;
                        p.Race=  theRace;
                        p.Class= theClass;
                        p.Birthday = rnd.Next(365);
                        p.luck = 0.9 + luckcalc;
                        rt.Add(p);
                         i++;
  
                         Console.WriteLine(p.luck);
   
            }   
            return rt ; 
        } 

        static void Produce(string topic, ClientConfig config)
        {
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                 Console.WriteLine("Produce");
                int numProduced = 0;
                int numMessages = int.MaxValue;
                int iSleep = 1000;

                Random rnd = new Random();
                int[] myDice = {4, 6, 8, 12, 20};
             /*   string[] myClasses= {"Barbarian","Bard","Cleric","Druid","Fighter","Monk","Paladin","Ranger","Rogue", "Sorcerer","Warlock","Wizard"};
                string[] myRaces = { "Dragonborn","Dwarf","Elf","Gnome","Half-Elf","Half-Orc","Halfling","Human","Tiefling"};
                var personGenerator = new PersonNameGenerator();
                var placeGenerator = new PlaceNameGenerator();
                var dt = new DateTime();
                */  // 2020-06-14T17:44:11.8027036
                DateTime localDate = DateTime.Parse("2020-06-16");
                List<player> playerList = new List<player>();
                //List<String> playername = new List<string>();

                double  secondstimer = 3600;
                int numberofplayers = 2000;
                Console.WriteLine("get characters");

                List<player>  gg =  globalplayers;

                var gameID = rnd.Next();

                var theplayersc = 0;    

                for (int i=0; i<numMessages; ++i)
                {
                    theplayersc++;
                    if(theplayersc > numberofplayers) theplayersc = numberofplayers - 500;
                    var playerid = rnd.Next(1,theplayersc);

                    Thread.Sleep(iSleep);
                    var key = "DiceRoll";
                    var dice6   = rnd.Next(1, 7);  
                    var randnumber = rnd.Next();

                    var iWhichDice = rnd.Next(myDice.Length);
                    var myDiceType = myDice[iWhichDice];
                    var myRoll = rnd.Next(0,myDiceType+1);
                    var itsmybirthday = rnd.Next(0,365);

                    if(rnd.Next(10)>=9) { gameID++; }
                
                    player currentplayer = gg[playerid];
                    var theClass = currentplayer.Class;
                    var place = currentplayer.Location;
                    var name = currentplayer.Name;
                    var theRace = currentplayer.Race;
                    var bday = currentplayer.Birthday;
                    if( itsmybirthday ==  localDate.DayOfYear) {  
                        //currentplayer.luck =currentplayer.luck + 2;
                        Console.WriteLine("*******************************Its my birthday");
                      }
                    myRoll = Convert.ToInt32 (  Convert.ToDouble (myRoll) * currentplayer.luck );

                    if( itsmybirthday ==  localDate.DayOfYear) {  
                        myRoll = myDiceType;
                      }

                    if(myRoll > myDiceType) myRoll = myDiceType;

                    var val = JObject.FromObject(new { Roll = myRoll , Dice = myDiceType ,
                     SystemID = randnumber, GameID = gameID , Class = theClass 
                     , Race = theRace, Name = name , Location = place , RollDateTime = localDate }).ToString(Formatting.None);

                    secondstimer = secondstimer / 1.001;
                    localDate = localDate.AddSeconds(secondstimer);

try {
                    Console.WriteLine($"Producing record: {key} {val}");

                    producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                numProduced += 1;
                            }
                        });

                       
                }
               catch
               {
                 Console.WriteLine("..");
               } 

                }
                
                producer.Flush(TimeSpan.FromSeconds(10));

                Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            }
        }



       
        static void Consume(string topic, ClientConfig config)
        {
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "dotnet-example-group-1";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = false;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                var totalCount = 0;
                try
                {
                    while (true)
                    {
                        try {
                            Thread.Sleep(100);
                        var cr = consumer.Consume(cts.Token);
                        totalCount += JObject.Parse(cr.Message.Value).Value<int>("count");

                        SendEHMessage(cr.Message.Value);
                        Console.WriteLine($"Consumed record with key {cr.Message.Key} and value {cr.Message.Value}, and updated total count to {totalCount}");
                    } catch(Exception ex) { Console.WriteLine(ex.Message); 
                    }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C was pressed.
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        static void PrintUsage()
        {
            Console.WriteLine("usage: .. produce|consume <topic> <configPath> [<certDir>]");
            System.Environment.Exit(1);
        }

 static async void SendEHMessagewl()
        {
            int y=0;
            int i=5000;
            while(y<i)
            {
                Console.WriteLine("Sending message {0}", y);
                string m = "{ col1:'t',col2:'y',id:1  }";
                SendEHMessage(m);
                Thread.Sleep(500);
                y++;
            }
        
        }


        static async void SendEHMessage(string message)
        {
            try {
            // number of events to be sent to the event hub
                // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
                // of the application, which is best practice when events are being published or read regularly.
                // TODO: Replace the <CONNECTION_STRING> and <HUB_NAME> placeholder values
                EventHubProducerClient producerClient = new EventHubProducerClient(
                    "Endpoint=sb://danddsqlbits.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=<KEY>;EntityPath=dandd" 
                    , "dandd");
                // Create a batch of events 
                
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                //message = "test,foo,fun";

                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"{message}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {message} is too large for the batch and cannot be sent.");
                }
            
                try
                {
                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"A batch of  events has been published.");
                }
                finally
                {
                    await producerClient.DisposeAsync();
                }
            }
            catch(Exception ex) 
            {
                Console.WriteLine(ex.Message);
            }
        }
      static List<player> globalplayers;

        static async Task Main(string[] args)
        {

         globalplayers = GetPlayersList(1000);


           // if (args.Length != 3 && args.Length != 4) { PrintUsage(); }
             Console.WriteLine("Main");
            var mode = args[0];
            var topic = args[1];
            var configPath = args[2];
            var certDir = args.Length == 4 ? args[3] : null;

            var config = await LoadConfig(configPath, certDir);

            switch (mode)
            {
                case "produce":
                    await CreateTopicMaybe(topic, 1, 3, config);
                    
                    while (1==1)
                    {
                    try {
                    Produce(topic, config);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    }
                    break;
                case "consume":
                    
                    while(1==1)
                    {
                    try {
                    Consume(topic, config);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    }
                    break;
                case "both":
                    await CreateTopicMaybe(topic, 1, 3, config);
                    Produce(topic, config);
                    Consume(topic, config);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
