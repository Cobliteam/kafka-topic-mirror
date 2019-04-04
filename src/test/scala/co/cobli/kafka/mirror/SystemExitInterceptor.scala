
package co.cobli.kafka.mirror

import java.security.Permission


/**
  * Hack to intercept Java System.exit() on tests
  */
trait SystemExitInterceptor {

  sealed case class ExitException(status: Int) extends SecurityException("System.exit() is not allowed")

  def setupSystemExitInterceptor(): Unit = {
    System.setSecurityManager(new SecurityManager {

      override def checkPermission(perm: Permission): Unit = {}

      override def checkPermission(perm: Permission, context: Object): Unit = {}

      override def checkExit(status: Int): Unit = {
        super.checkExit(status)
        throw ExitException(status)
      }
    })
  }

  def dismissSystemExitInterceptor(): Unit = System.setSecurityManager(null)
}