namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Darkseid")>]
[<assembly: AssemblyProductAttribute("Darkseid")>]
[<assembly: AssemblyDescriptionAttribute("Actor-based library to help you build a real-time data producing application on top of Amazon Kinesis")>]
[<assembly: AssemblyVersionAttribute("1.0.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.0"
