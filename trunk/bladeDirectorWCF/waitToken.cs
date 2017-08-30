namespace bladeDirectorWCF
{
    public class waitToken
    {
        public handleTypes handleType;
        public string t;

        public waitToken()
        {
            
        }

        public waitToken(handleTypes newHandleType, string newT)
        {
            handleType = newHandleType;
            t = newT;
        }

        public override int GetHashCode()
        {
            return handleType.GetHashCode() ^ t.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            waitToken objTyped = obj as waitToken;
            if (objTyped == null)
                return false;

            if (!handleType.Equals(objTyped.handleType) ||
                !t.Equals(objTyped.t))
            {
                return false;
            }

            return true;
        }
    }
}