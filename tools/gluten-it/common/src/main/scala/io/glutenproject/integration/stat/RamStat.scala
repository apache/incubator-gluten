package io.glutenproject.integration.stat

import java.io.InputStreamReader
import java.lang.management.ManagementFactory
import java.util.{Scanner, StringTokenizer}

object RamStat {

  private def getPid(): String = {
    val beanName = ManagementFactory.getRuntimeMXBean.getName
    beanName.substring(0, beanName.indexOf('@'))
  }

  def getJvmHeapUsed(): Long = {
    val heapTotalBytes = Runtime.getRuntime.totalMemory()
    val heapUsed = (heapTotalBytes - Runtime.getRuntime.freeMemory()) / 1024L
    heapUsed
  }

  def getJvmHeapTotal(): Long = {
    val heapTotalBytes = Runtime.getRuntime.totalMemory()
    val heapTotal = heapTotalBytes / 1024L
    heapTotal
  }

  def getProcessRamUsed(): Long = {
    val proc = Runtime.getRuntime.exec("ps -p " + getPid() + " -o rss")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return;
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    scanner.nextLine().toLong
  }

  def getSystemRamUsed(): Long = {
    val proc = Runtime.getRuntime.exec("free")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return;
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    val memLine = scanner.nextLine()
    val tok = new StringTokenizer(memLine)
    tok.nextToken()
    tok.nextToken()
    tok.nextToken().toLong
  }
}
