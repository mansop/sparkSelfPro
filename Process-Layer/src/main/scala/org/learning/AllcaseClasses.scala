package org.learning

import java.lang.Float

case class City(id: Int, name: String, countrycode: String, district: String, population: Int)
case class Station(id: Int, city: String, state: String, lat_n: Double, long_w: Double)
case class Country(code: String, name: String, continent: String, region: String, surfacearea: Double, indepyear: String, population: Int, lifeexpectancy: String, gnp: Double, gnpold: String, localname: String, governmentform: String, headofstate: String, capital: String, code2: String)
case class Grade(grade: String, min_mark: Int, max_mark: Int)
case class Student(id: String, name: String , mark: Int)