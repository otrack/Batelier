package net.sourceforge.fractal.replication.database.certificationprotocol.trivial;

import net.sourceforge.fractal.replication.database.PSTORE;
import net.sourceforge.fractal.replication.database.CertificationProtocol;
import net.sourceforge.fractal.replication.database.TpcbTransaction;
/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/

public class TrivialCertificationProtocol extends CertificationProtocol {

	public TrivialCertificationProtocol(PSTORE pstore) {
		super(pstore);
	}

	@Override
	public void submit(TpcbTransaction t) {
		commit(t);
	}

	@Override
	public void start() {}

	@Override
	public void stop() {}

}
