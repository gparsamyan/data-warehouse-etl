#!/usr/bin/env ruby

require 'csv'
require 'json'

CSV.open('report.csv', 'w') do |csv|
  csv << ["rid", "foodspotting", "interior", "exterior", "food", "drink", "staff", "other"]

  contents = File.read("counts.json")
  json = JSON.parse(contents)

  json.each do |row|
    rid = row["public.rid"]
    foodspotting = row["score.count.foodspotting"] || 0

    interior = row["score.count.opentable.interior"] || 0
    exterior = row["score.count.opentable.exterior"] || 0
    food = row["score.count.opentable.food"] || 0
    drink = row["score.count.opentable.drink"] || 0
    staff = row["score.count.opentable.staff"] || 0
    other = row["score.count.opentable.other"] || 0

    total = interior + exterior + food + drink + staff + other

    csv << [rid, foodspotting, interior, exterior, food, drink, staff, other]
  end

end
