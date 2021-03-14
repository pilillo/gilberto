package com.github.pilillo

trait Validator[D] {
  def open : D
  def validate : Boolean
}
