using System;
using System.Collections.Generic;
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Web;

namespace bladeDirectorWCF
{
    [ServiceContract]
    public interface IWebServices
    {
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Xml, BodyStyle = WebMessageBodyStyle.Bare)]
        Stream generateIPXEScript();
    }

    [ServiceContract]
    [ServiceKnownType(typeof(resultAndBladeName))]
    [ServiceKnownType(typeof(resultAndBIOSConfig))]
    public interface IServices
    {
        [OperationContract]
        void keepAlive(string srcIP);

        [OperationContract]
        resultAndWaitToken logIn();

        [OperationContract]
        resultAndWaitToken getProgress(string waitToken);

        [OperationContract]
        string[] getAllBladeIP();

        [OperationContract]
        resultAndWaitToken RequestAnySingleNode();

        [OperationContract]
        GetBladeStatusResult GetBladeStatus(string nodeIP);

        [OperationContract]
        resultAndWaitToken ReleaseBladeOrVM(string nodeIP, bool force);

        [OperationContract]
        bool isBladeMine(string nodeIP);

        [OperationContract]
        resultAndWaitToken rebootAndStartDeployingBIOSToBlade(string NodeIP, string BIOSXML);

        [OperationContract]
        resultAndWaitToken rebootAndStartReadingBIOSConfiguration(string NodeIP);

        [OperationContract]
        resultAndBladeName RequestAnySingleVM(VMHardwareSpec hwSpec, VMSoftwareSpec swSpec);

        [OperationContract]
        string generateIPXEScript();
        
        [OperationContract]
        TimeSpan getKeepAliveTimeout();

        [OperationContract]
        resultCode addNode(string nodeIP, string iSCSIIP, string iLoIP, ushort debugPort);

        [OperationContract]
        vmSpec getVMByIP_withoutLocking(string VMIP);

        [OperationContract]
        bladeSpec getBladeByIP_withoutLocking(string bladeIP);

        [OperationContract]
        string[] getBladesByAllocatedServer(string serverIP);

        [OperationContract]
        vmSpec[] getVMByVMServerIP_nolocking(string bladeIP);

        [OperationContract]
        string[] getLogEvents();
    }
}