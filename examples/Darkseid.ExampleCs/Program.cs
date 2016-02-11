using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon;

using Darkseid.Model;

using log4net.Config;

namespace Darkseid.ExampleCs
{
    class Program
    {
        static void Main(string[] args)
        {
            var awsKey = "AKIAJ27RLPDDIA5Z2NLQ";
            var awsSecret = "mZMMsmmhZeF78+2t7G807GnKtc8uKjUX/BHI/mLo";
            var region = RegionEndpoint.USEast1;
            var appName = "YC-TEST";
            var streamName = "YC-test";

            BasicConfigurator.Configure();

            var bgConfig = new BackgroundProcessingConfig
                {
                    HighWaterMarks = 100000,
                    HighWaterMarksMode = HighWaterMarksMode.Block
                };
            var mode = ProcessingMode.NewBackground(bgConfig);
            var config = new DarkseidConfig { Mode = mode, LevelOfConcurrency = 100 };

            var producer = Producer.CreateNew(awsKey, awsSecret, region, appName, streamName, config);

            var rawPayload = string.Join("", Enumerable.Range(1, 3).Select(_ => "42"));
            var payload = Encoding.UTF8.GetBytes(rawPayload);

            Console.WriteLine("Starting 10 send loops...");

            Enumerable
                .Range(1, 10)
                .ToList()
                .ForEach(_ => Task.Run(() => SendLoop(producer, payload)));

            Console.WriteLine("Started.");
            Console.WriteLine("Press any key to stop...");

            Console.ReadKey();
        }

        private static async Task SendLoop(IProducer producer, byte[] payload)
        {
            while (true)
            {
                var record = new Record(payload, Guid.NewGuid().ToString());
                await producer.Send(record);
                await Task.Delay(1);
            }
        }
    }
}
