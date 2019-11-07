package com.meituan.meishi.data.lqy.scala.examples.tour

import scala.util.Random

object Matches {

  def main(args: Array[String]): Unit = {
    val x = Random.nextInt(10)
    val s = x match {
      case 0 => "zero"
      case 1 => "one"
      case 2 => "two"
      case _ => "other"
    }
    println(s)

//    val someSms = SMS("12345", "Are you there?")
//    val someVoiceRecording = VoiceRecording("Tom", "voicerecording.org/id/123")
//    println(showNotification(someSms))
//    println(showNotification(someVoiceRecording))

    val importantPeopleInfo = Seq("867-5309", "jenny@gmail.com")
    val someSms = SMS("867-5309", "Are you there?")
    val someVoiceRecording = VoiceRecording("Tom", "voicerecording.org/id/123")
    val importantEmail = Email("jenny@gmail.com", "Drinks tonight?", "I'm free after 5!")
    val importantSms = SMS("867-5309", "I'm here! Where are you?")

    println(showImportantNotification(someSms, importantPeopleInfo))
    println(showImportantNotification(someVoiceRecording, importantPeopleInfo))
    println(showImportantNotification(importantEmail, importantPeopleInfo))
    println(showImportantNotification(importantSms, importantPeopleInfo))
  }

  abstract class Notification

  case class Email(sender: String, title: String, body: String) extends Notification

  case class SMS(cellar: String, message: String) extends Notification

  case class VoiceRecording(contactName: String, link: String) extends Notification

  def showNotification(notification: Notification): String = {
    notification match {
      case Email(sender, title, _) => s"You got an email from $sender with title: $title"
      case SMS(number, message) =>
        s"You got an SMS from $number! Message: $message"
      case VoiceRecording(name, link) =>
        s"you received a Voice Recording from $name! Click the link to hear it: $link"
    }
  }

  def showImportantNotification(notification: Notification, importantPeopleInfo: Seq[String]): String = {
    notification match {
      case Email(sender, _, _) if importantPeopleInfo.contains(sender) =>
        "You got an email from special someone!"
      case SMS(number, _) if importantPeopleInfo.contains(number) =>
        "You got an SMS from special someone!"
      case other =>
        showNotification(other) // nothing special, delegate to our original showNotification function
    }
  }


  abstract class Device

  case class Phone(model: String) extends Device {
    def screenOff = "Turning screen off"
  }

  case class Computer(model: String) extends Device {
    def screenSaverOn = "Turning screen saver on..."
  }

  //仅匹配类型
  def goIdle(device: Device) = device match {
    case p: Phone => p.screenOff
    case c: Computer => c.screenSaverOn
  }
}
