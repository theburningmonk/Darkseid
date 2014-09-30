namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Darkseid")>]
[<assembly: AssemblyProductAttribute("Darkseid")>]
[<assembly: AssemblyDescriptionAttribute("Actor-based library to help you build a real-time data producing application on top of Amazon Kinesis")>]
[<assembly: AssemblyVersionAttribute("0.2.0")>]
[<assembly: AssemblyFileVersionAttribute("0.2.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.2.0"
