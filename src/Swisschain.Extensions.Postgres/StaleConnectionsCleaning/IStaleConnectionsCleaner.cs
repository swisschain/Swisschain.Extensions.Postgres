using System.Threading.Tasks;

namespace Swisschain.Extensions.Postgres.StaleConnectionsCleaning
{
    internal interface IStaleConnectionsCleaner
    {
        Task Clear();
    }
}
