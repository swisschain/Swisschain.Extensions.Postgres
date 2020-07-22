using System.Threading.Tasks;

namespace Swisschain.Extensions.Postgres.StaleConnectionsCleaning
{
    public interface IStaleConnectionsCleaner
    {
        Task Clear();
    }
}
