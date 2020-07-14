package org.inceptez.hack

class AllMethods extends java.io.Serializable {
  def remspecialchar(i: String):String = {
    return (i.replaceAll("[^a-zA-Z\\s]", "").replaceAll(" +", " "))
  }
}